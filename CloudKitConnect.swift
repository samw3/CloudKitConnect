//
//  CloudKitConnect.swift
//  StripboardDesigner
//
//  Created by Sam Washburn on 2/26/19.
//  Copyright © 2019 Sam Washburn. All rights reserved.
//

import Foundation
import CloudKit
import GRDB

struct CKOperationContainer {
    var op: CKOperation
}

class CloudKitConnect {
    var isInitialized = false

    let container: CKContainer
    let publicDB: CKDatabase
    let privateDB: CKDatabase
    let sharedDB: CKDatabase
    let privateZoneNames: [String]
    let publicRecordTypes: Set<String>;
    let dbPool: DatabasePool

    var createdPrivateZones = UserDefaults.standard.bool(forKey: "createdPrivateZones")
    var subscribedToPrivateChanges = UserDefaults.standard.bool(forKey: "subscribedToPrivateChanges")
    var subscribedToSharedChanges = UserDefaults.standard.bool(forKey: "subscribedToSharedChanges")
    var subscribedToPublicChanges: Set = Set<String>()

    let privateSubscriptionID = "private-changes"
    let sharedSubscriptionID = "shared-changes"

    init(privateZoneNames: [String], publicRecordTypes: [String]) throws {
        container = CKContainer.default()
        publicDB = container.publicCloudDatabase
        privateDB = container.privateCloudDatabase
        sharedDB = container.sharedCloudDatabase
        self.privateZoneNames = privateZoneNames
        self.publicRecordTypes = Set(publicRecordTypes)
        let databaseURL = try FileManager.default
            .url(for: .applicationSupportDirectory, in: .userDomainMask, appropriateFor: nil, create: true)
            .appendingPathComponent("local_cache.sqlite")
        dbPool = try DatabasePool(path: databaseURL.path)

        publicRecordTypes.forEach {
            if UserDefaults.standard.bool(forKey: "subscribedToPublicZone-\($0)") {
                subscribedToPublicChanges.insert($0)
            }
        }

        try self.scaffold()
    }

    func scaffold() throws {
        dlog("Begin scaffolding")

        // Init local repository
        try dbPool.write { db in
            if try db.tableExists("records") == false {
                try db.create(table: "records") { t in
                    t.autoIncrementedPrimaryKey("id")
                    t.column("db_key", .text).notNull()
                    t.column("record_name", .text).defaults(to: DatabaseValue.null)
                    t.column("zone_id", .text).defaults(to: DatabaseValue.null)
                    t.column("modified", .date).notNull()
                    t.column("ck_record", .blob).notNull()
                }
                try db.create(
                    index: "ck_index",
                    on: "records",
                    columns: [
                        "db_key", "record_name", "zone_id"
                    ],
                    unique: true,
                    ifNotExists: true
                )
            }
        }

        // Init CloudKit
        let cloudKitInitialized = DispatchGroup()
        cloudKitInitialized.enter()

        createPrivateZones(dispatchGroup: cloudKitInitialized)
        subscribeToPrivateChanges(dispatchGroup: cloudKitInitialized);
        subscribeToSharedChanges(dispatchGroup: cloudKitInitialized);
        let seqGroup = DispatchGroup()
        publicRecordTypes.forEach {
            subscribeToPublicChanges(recordType: $0, dispatchGroup: seqGroup)
            seqGroup.wait()
        }

        cloudKitInitialized.leave()
        cloudKitInitialized.notify(queue: DispatchQueue.global()) {
            if self.createdPrivateZones {
                self.fetchChanges()
            }
        }

        dlog("End scaffolding")
    }

    func modifyRecords(_ records: [CKRecord]?, andDelete deleteIds: [CKRecord.ID]?, in db: CKDatabase, completionHandler: @escaping ([CKRecord]?, [CKRecord.ID]?, Error?) -> Void) {
        let op = CKModifyRecordsOperation(recordsToSave: records, recordIDsToDelete: deleteIds)
        op.savePolicy = .allKeys
        op.modifyRecordsCompletionBlock = { (_ savedRecords: [CKRecord]?, _ deletedRecordIds: [CKRecord.ID]?, _ operationError: Error?) -> Void in
            var returnError = operationError
            if let ckerror = operationError as? CKError {
                switch ckerror {
                case CKError.requestRateLimited,
                     CKError.serviceUnavailable,
                     CKError.zoneBusy,
                     CKError.networkUnavailable,
                     CKError.networkFailure:
                    let retry = ckerror.retryAfterSeconds ?? 3.0
                    DispatchQueue.global().asyncAfter(deadline: DispatchTime.now() + retry, execute: {
                        self.modifyRecords(records, andDelete: deleteIds, in: db, completionHandler: completionHandler)
                    })

                    return
                case CKError.partialFailure:
                    if (savedRecords != nil && savedRecords!.count > 0) ||
                        (deletedRecordIds != nil && deletedRecordIds!.count > 0)
                    {
                        returnError = nil
                    }
                default:
                    break
                }
            }

            completionHandler(savedRecords, deletedRecordIds, returnError)
        }
        db.add(op)
    }

    func lastestRecordDate(dbKey: String) throws -> Date {
        var latest: Date?
        try dbPool.read { db in
            latest = try Date.fetchOne(db, "SELECT modified FROM records WHERE db_key=? ORDER BY modified DESC LIMIT 1", arguments: [dbKey])
        }
        return latest ?? Date(timeIntervalSince1970: 0)
    }

    func handlePushNotification(userInfo: [AnyHashable: Any]) throws {
        let dict = userInfo as! [String: NSObject]
        let notification = CKNotification(fromRemoteNotificationDictionary: dict)!
        switch notification.notificationType {
        case .query:
            let ckqn = notification as! CKQueryNotification
            switch ckqn.databaseScope {
            case .private:
                fetchChanges(in: ckqn.databaseScope) { }
                break
            case .shared:
                fetchChanges(in: ckqn.databaseScope) { }
                break
            case .public:
                switch ckqn.queryNotificationReason {
                case .recordDeleted:
                    recordDeleted(recordID: ckqn.recordID!, dbKey: "public")
                    break
                case .recordCreated:
                    fallthrough
                case .recordUpdated:
                    fetchChanges(in: ckqn.databaseScope) { }
                    @unknown default:
                    // If the notification is unknown, probably best to
                    // just fetch any changes
                    fetchChanges(in: ckqn.databaseScope) { }
                }
                break
                @unknown default:
                dlog("Unsupported database scope")
            }
            break
        case .database:
            let ckdn = notification as! CKDatabaseNotification
            switch ckdn.databaseScope {
            case .private:
                break
            case .shared:
                break
            case .public:
                dlog("Not supported by apple...")
                break
                @unknown default:
                dlog("Unsupported database scope")
            }
            break
        case .readNotification:
            break
        case .recordZone:
            break
            @unknown default:
            dlog("Unknown/unhandled notification type")
            break
        }
    }

    func fetchPublicChangesSince(recordType: CKRecord.RecordType, since: Date) {
        let q = CKQuery.init(recordType: recordType, predicate: NSPredicate.init(format: "modificationDate > %@", since as NSDate))
        let op = CKQueryOperation.init(query: q)
        op.qualityOfService = .utility
        publicDB.perform(q, inZoneWith: nil) { (results, error) in
            guard error == nil else {
                dlog(String(describing: error))
                return;
            }
            guard let results = results else { return }
            for record in results {
                self.recordChanged(record: record, dbKey: "public")
            }
        }
    }

    func fetchChanges() {
        self.fetchChanges(in: .private) { }
        self.fetchChanges(in: .shared) { }
        self.fetchChanges(in: .public) { }
    }

    func fetchChanges(in databaseScope: CKDatabase.Scope, completion: @escaping () -> Void) {
        switch databaseScope {
        case .private:
            fetchDatabaseChanges(database: self.privateDB, databaseTokenKey: "private", completion: completion)
        case .shared:
            fetchDatabaseChanges(database: self.sharedDB, databaseTokenKey: "shared", completion: completion)
        case .public:
            do {
                let latest = try lastestRecordDate(dbKey: "public")
                publicRecordTypes.forEach {
                    fetchPublicChangesSince(recordType: $0, since: latest)
                }
            } catch let e {
                dlog(String(describing: e))
            }
        @unknown default:
            dlog("Unsupported database scope")
        }
    }

    func fetchDatabaseChanges(database: CKDatabase, databaseTokenKey: String, completion: @escaping () -> Void) {
        let saveKey = "cloudKitChangeToken-DB-\(databaseTokenKey)"
        var changedZoneIDs: [CKRecordZone.ID] = []

        let changeToken = unserializeToToken(UserDefaults.standard.string(forKey: saveKey) ?? "")
        let operation = CKFetchDatabaseChangesOperation(previousServerChangeToken: changeToken)

        operation.recordZoneWithIDChangedBlock = { (zoneID) in
            changedZoneIDs.append(zoneID)
        }

        operation.recordZoneWithIDWasDeletedBlock = { (zoneID) in
            self.zoneDeleted(zoneID, dbKey: databaseTokenKey)
        }

        operation.changeTokenUpdatedBlock = { (token) in
            UserDefaults.standard.set(self.serialize(token: token), forKey: saveKey)
        }

        operation.fetchDatabaseChangesCompletionBlock = { (token, moreComing, error) in
            if let error = error {
                dlog("Error during fetch shared database changes operation \(error)")
                if let ckerror = error as? CKError {
                    switch ckerror {
                    case CKError.requestRateLimited,
                         CKError.serviceUnavailable,
                         CKError.zoneBusy,
                         CKError.networkUnavailable,
                         CKError.networkFailure:
                        let retry = ckerror.retryAfterSeconds ?? 3.0
                        DispatchQueue.global().asyncAfter(deadline: DispatchTime.now() + retry, execute: {
                            self.fetchDatabaseChanges(database: database, databaseTokenKey: databaseTokenKey, completion: completion)
                        })
                        return
                    case CKError.partialFailure:
                        // TODO: Handle partial failures
                        fatalError()
                    default:
                        break
                    }
                }
                completion()
                return
            }
            UserDefaults.standard.set(self.serialize(token: token), forKey: saveKey)

            // Fetch Zone Changes
            if changedZoneIDs.isEmpty { return }

            // Look up the previous change token for each zone
            var configByRecordZoneID = [CKRecordZone.ID: CKFetchRecordZoneChangesOperation.ZoneConfiguration]()
            for zoneID in changedZoneIDs {
                let saveKey = "cloudKitChangeToken-DB-\(databaseTokenKey)-\(zoneID)"
                let options = CKFetchRecordZoneChangesOperation.ZoneConfiguration()
                options.previousServerChangeToken = self.unserializeToToken(UserDefaults.standard.string(forKey: saveKey) ?? "")
                configByRecordZoneID[zoneID] = options
            }
            let operation = CKFetchRecordZoneChangesOperation(recordZoneIDs: changedZoneIDs, configurationsByRecordZoneID: configByRecordZoneID)

            operation.recordChangedBlock = { (record) in
                self.recordChanged(record: record, dbKey: databaseTokenKey)
            }

            operation.recordWithIDWasDeletedBlock = { (recordID, recordType) in
                self.recordDeleted(recordID: recordID, dbKey: databaseTokenKey)
            }

            operation.recordZoneChangeTokensUpdatedBlock = { (zoneID, token, data) in
                let saveKey = "cloudKitChangeToken-DB-\(databaseTokenKey)-\(zoneID)"
                UserDefaults.standard.set(self.serialize(token: token), forKey: saveKey)
            }

            operation.recordZoneFetchCompletionBlock = { (zoneID, token, _, _, error) in
                if let error = error {
                    dlog("Error fetching zone changes for \(databaseTokenKey) database: \(error)")
                    return
                }
                let saveKey = "cloudKitChangeToken-DB-\(databaseTokenKey)-\(zoneID)"
                UserDefaults.standard.set(self.serialize(token: token), forKey: saveKey)
            }

            operation.fetchRecordZoneChangesCompletionBlock = { (error) in
                if let error = error {
                    dlog("Error fetching zone changes for \(databaseTokenKey) database: \(error)")
                }
                completion()
            }
            database.add(operation)
            completion()
        }
        operation.qualityOfService = .userInitiated

        database.add(operation)
    }

    func serialize(token: CKServerChangeToken?) -> String {
        guard let token = token else {
            return "";
        }
        let string: String
        do {
            let data = try NSKeyedArchiver.archivedData(withRootObject: token, requiringSecureCoding: true)
            string = data.base64EncodedString()
        } catch {
            string = ""
        }
        return string
    }

    func unserializeToToken(_ string: String) -> CKServerChangeToken? {
        guard let data = Data(base64Encoded: string, options: []) else {
            return nil
        }
        let token: CKServerChangeToken?
        do {
            token = try NSKeyedUnarchiver.unarchivedObject(ofClass: CKServerChangeToken.self, from: data)
        } catch {
            token = nil
        }
        return token
    }

    func serialize(record: CKRecord?) -> String {
        guard let record = record else {
            return "";
        }
        let string: String
        do {
            let data = try NSKeyedArchiver.archivedData(withRootObject: record, requiringSecureCoding: true)
            string = data.base64EncodedString()
        } catch {
            string = ""
        }
        return string
    }

    func unserializeToRecord(_ string: String) -> CKRecord? {
        guard let data = Data(base64Encoded: string, options: []) else {
            return nil
        }
        let record: CKRecord?
        do {
            record = try NSKeyedUnarchiver.unarchivedObject(ofClass: CKRecord.self, from: data)
        } catch {
            record = nil
        }
        return record
    }

    func createPrivateZones(dispatchGroup: DispatchGroup) {
        if !self.createdPrivateZones {
            dispatchGroup.enter()
            var zones: [CKRecordZone] = []
            for zoneName in privateZoneNames {
                let zoneID = CKRecordZone.ID(zoneName: zoneName, ownerName: CKCurrentUserDefaultName)
                let zone = CKRecordZone(zoneID: zoneID)
                zones.append(zone)
            }

            let createZoneOperation = CKModifyRecordZonesOperation(recordZonesToSave: zones, recordZoneIDsToDelete: [])
            createZoneOperation.modifyRecordZonesCompletionBlock = { (saved, deleted, error) in
                if error == nil {
                    self.createdPrivateZones = true
                    UserDefaults.standard.set(self.createdPrivateZones, forKey: "createdPrivateZones")
                } else {
                    dlog(String(describing: error))
                }
                dispatchGroup.leave()
            }
            createZoneOperation.qualityOfService = .userInitiated
            self.privateDB.add(createZoneOperation)
        }
    }

    func subscribeToPrivateChanges(dispatchGroup: DispatchGroup) {
        if self.subscribedToPrivateChanges { return }
        dispatchGroup.enter()
        let createSubscriptionOperation = self.createDatabaseSubscriptionOperation(subscriptionID: privateSubscriptionID)
        createSubscriptionOperation.modifySubscriptionsCompletionBlock = { (subscriptions, deletedIDs, error) in
            if error == nil {
                self.subscribedToPrivateChanges = true
                UserDefaults.standard.set(self.subscribedToPrivateChanges, forKey: "subscribedToPrivateChanges")
                dlog("Subscribed to private changes.")
            } else {
                dlog(String(describing: error))
            }
            dispatchGroup.leave()
        }
        self.privateDB.add(createSubscriptionOperation)
    }

    func subscribeToSharedChanges(dispatchGroup: DispatchGroup) {
        if self.subscribedToSharedChanges { return }
        dispatchGroup.enter()
        let createSubscriptionOperation = self.createDatabaseSubscriptionOperation(subscriptionID: sharedSubscriptionID)
        createSubscriptionOperation.modifySubscriptionsCompletionBlock = { (subscriptions, deletedIDs, error) in
            if error == nil {
                self.subscribedToSharedChanges = true
                UserDefaults.standard.set(self.subscribedToSharedChanges, forKey: "subscribedToSharedChanges")
                dlog("Subscribed to shared changes.")
            } else {
                dlog(String(describing: error))
            }
            dispatchGroup.leave()
        }
        self.sharedDB.add(createSubscriptionOperation)
    }

    func subscribeToPublicChanges(recordType: String, dispatchGroup: DispatchGroup) {
        if self.subscribedToPublicChanges.contains(recordType) { return }
        dispatchGroup.enter()
        let subscriptionID = "public-changes-\(recordType)"
        let createSubscriptionOperation = self.createRecordTypeSubscriptionOperation(subscriptionID: subscriptionID, recordType: recordType)
        createSubscriptionOperation.modifySubscriptionsCompletionBlock = { (subscriptions, deletedIDs, error) in
            if error == nil {
                self.subscribedToPublicChanges.insert(recordType)
                UserDefaults.standard.set(true, forKey: "subscribedToPublicZone-\(recordType)")
                dlog("Subscribed to public zone: \(recordType)")
            } else {
                dlog(String(describing: error))
            }
            dispatchGroup.leave()
        }
        self.publicDB.add(createSubscriptionOperation)
    }

    func createRecordTypeSubscriptionOperation(subscriptionID: String, recordType: String) -> CKModifySubscriptionsOperation {
        let subscriptionOptions: CKQuerySubscription.Options = [.firesOnRecordCreation, .firesOnRecordUpdate, .firesOnRecordDeletion]
        let subscription = CKQuerySubscription(
            recordType: recordType,
            predicate: NSPredicate.init(value: true),
            subscriptionID: subscriptionID,
            options: subscriptionOptions
        )

        let notificationInfo = CKSubscription.NotificationInfo()
        // send a silent notification
        notificationInfo.shouldSendContentAvailable = true
        subscription.notificationInfo = notificationInfo

        let operation = CKModifySubscriptionsOperation(subscriptionsToSave: [subscription], subscriptionIDsToDelete: [])
        operation.qualityOfService = .utility

        return operation
    }

    func createZoneSubscriptionOperation(subscriptionID: String, zoneID: CKRecordZone.ID) -> CKModifySubscriptionsOperation {
        let subscription = CKRecordZoneSubscription.init(zoneID: zoneID, subscriptionID: subscriptionID)

        let notificationInfo = CKSubscription.NotificationInfo()
        // send a silent notification
        notificationInfo.shouldSendContentAvailable = true
        subscription.notificationInfo = notificationInfo

        let operation = CKModifySubscriptionsOperation(subscriptionsToSave: [subscription], subscriptionIDsToDelete: [])
        operation.qualityOfService = .utility

        return operation
    }

    func createDatabaseSubscriptionOperation(subscriptionID: String) -> CKModifySubscriptionsOperation {
        let subscription = CKDatabaseSubscription.init(subscriptionID: subscriptionID)

        let notificationInfo = CKSubscription.NotificationInfo()
        // send a silent notification
        notificationInfo.shouldSendContentAvailable = true
        subscription.notificationInfo = notificationInfo

        let operation = CKModifySubscriptionsOperation(subscriptionsToSave: [subscription], subscriptionIDsToDelete: [])
        operation.qualityOfService = .utility

        return operation
    }

    func zoneDeleted(_ zoneID: CKRecordZone.ID, dbKey: String) {
        dlog("Zone deleted: \(zoneID)")
        // if a zone deletion is detected, delete the whole database
        UserDefaults.standard.removeObject(forKey: "createdPrivateZones")
        UserDefaults.standard.removeObject(forKey: "subscribedToPrivateChanges")
        UserDefaults.standard.removeObject(forKey: "subscribedToSharedChanges")
        UserDefaults.standard.removeObject(forKey: "cloudKitChangeToken-DB-\(dbKey)")

        let cloudKitDelete = DispatchGroup()
        cloudKitDelete.enter()

        unsubscribeFromDatabase(privateDB, cloudKitDelete)
        unsubscribeFromDatabase(sharedDB, cloudKitDelete)

        // Reset flags
        createdPrivateZones = false;
        subscribedToPrivateChanges = false;
        subscribedToSharedChanges = false;

        // Drop local data
        do {
            try dbPool.write { db in
                if try db.tableExists("records") {
                    try db.drop(table: "records")
                }
            }
        } catch let de as DatabaseError {
            dlog(de.description)
        } catch let e {
            dlog(e.localizedDescription)
        }

        cloudKitDelete.leave()

        cloudKitDelete.notify(queue: DispatchQueue.global()) {
            // Re-scaffold cloudkit
            do {
                try self.scaffold()
            } catch let de as DatabaseError {
                dlog(de.description)
            } catch let e {
                dlog(e.localizedDescription)
            }
        }
    }

    func unsubscribeFromDatabase(_ db: CKDatabase, _ dispatchGroup: DispatchGroup) {
        dispatchGroup.enter()
        db.fetchAllSubscriptions { (subs, error) in
            subs?.forEach { sub in
                db.delete(withSubscriptionID: sub.subscriptionID) { (subscriptionID, error) in
                    // TODO : This assumes this works
                    dispatchGroup.leave()
                }
            }
        }
    }

    func recordChanged(record: CKRecord, dbKey: String) {
        dlog("record changed \(record))")
        do {
            try dbPool.write { db in
                let serializedRecord = serialize(record: record)
                try db.execute(
                    """
                    UPDATE OR IGNORE records SET modified=?, ck_record=? WHERE db_key=? AND record_name=? AND zone_id=?;
                    INSERT OR IGNORE INTO records (db_key, record_name, zone_id, modified, ck_record) VALUES (?, ?, ?, ?, ?)
                    """,
                    arguments: [
                        record.modificationDate,
                        serializedRecord,
                        dbKey,
                        record.recordID.recordName,
                        record.recordID.zoneID.zoneName,

                        dbKey,
                        record.recordID.recordName,
                        record.recordID.zoneID.zoneName,
                        record.modificationDate,
                        serializedRecord
                    ]
                )
            }
        } catch let de as DatabaseError {
            dlog(de.description)
        } catch let e {
            dlog(e.localizedDescription)
        }
    }

    func recordDeleted(recordID: CKRecord.ID, dbKey: String) {
        dlog("record deleted \(recordID))")
        do {
            try dbPool.write { db in
                try db.execute(
                    """
                    DELETE FROM records WHERE db_key=? AND record_name=? AND zone_id=?;
                    """,
                    arguments: [
                        dbKey,
                        recordID.recordName,
                        recordID.zoneID.zoneName,
                    ]
                )
            }
        } catch let de as DatabaseError {
            dlog(de.description)
        } catch let e {
            dlog(e.localizedDescription)
        }
    }
}
