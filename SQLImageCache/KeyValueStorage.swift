//
//  KeyValueStorage.swift
//  SQLImageCache
//
//  Created by songzhou on 2020/5/29.
//  Copyright © 2020 songzhou. All rights reserved.
//

import Foundation
import SQLite3
import var CommonCrypto.CC_MD5_DIGEST_LENGTH
import func CommonCrypto.CC_MD5
import typealias CommonCrypto.CC_LONG

private let dbFileName = "manifest.sqlite"
private let dataDirectoryName = "data"

/**
 
 File:
 /path/
      /manifest.sqlite
      /data/
           /e10adc3949ba59abbe56e057f20f883e
           /e10adc3949ba59abbe56e057f20f883e
 */
class KVStorageItem: NSObject {
    var key: String?
    var value: Data?
    
    var filename: String?
    
    var size: Int = 0
    var modifyTime: TimeInterval = 0
    var accessTime: TimeInterval = 0
}

class KVStorage: NSObject {
    init?(path: String) {
        self.path = path
        
        let pathURL = URL(fileURLWithPath: path)
        self.dbURL = pathURL.appendingPathComponent(dbFileName)
        self.dataURL = pathURL.appendingPathComponent(dataDirectoryName)
        
        do {
            try FileManager.default.createDirectory(atPath: path,
                                                    withIntermediateDirectories: true,
                                                    attributes: nil)
            
            try FileManager.default.createDirectory(at: self.dataURL,
                                                    withIntermediateDirectories: true,
                                                    attributes: nil)

            
            try db = SQLiteDatabase.open(path: self.dbURL.relativePath)
            try db.createTable(table: KVStorage.self)
        } catch (let error)  {
            print("KVStorage creation error:\(error)")
            return nil
        }
        
        super.init()
    }
    
    func saveItem(key: String, value: Data, filename: String) -> Bool {
        if key.count == 0 || value.count == 0 || filename.count == 0 {
            return false
        }
        
        do {
            try fileWrite(filename: filename, data: value)
        } catch {
            print("saveItem key:\(key) filename:\(filename) value:\(value) error: \(error)")
            return false
        }
        
        do {
            try dbSave(key: key, value: value, filename: filename)
        } catch {
            try? fileDelete(filename: filename)
            return false
        }
        
        return true
    }
    
    func getItem(key:String) -> KVStorageItem? {
        guard let item = dbGet(key: key) else { return nil }
        
        try? dbUpdateAccessTime(key: key)
        
        if let filename = item.filename {
            do {
                item.value = try fileRead(filename: filename)
            } catch {
                try? dbDeleteItem(key: key)
                return nil
            }
        }
        
        return item
    }
    
    // MARK: - DataBase -
    private func dbSave(key: String, value: Data, filename: String) throws {
        let sql = """
insert or replace into manifest (key, filename, size, modification_time, last_access_time) values (?1, ?2, ?3, ?4, ?5);
"""
        let statement = try db.prepareStatement(sql: sql)
        defer {
            db.finalize(sql: statement)
        }
        
        let timestamp = Int32(time(nil))
        
        sqlite3_bind_text(statement, 1, (key as NSString).utf8String, -1, nil)
        sqlite3_bind_text(statement, 2, (filename as NSString).utf8String, -1, nil)
        sqlite3_bind_int(statement, 3, Int32(value.count))
        sqlite3_bind_int(statement, 4, timestamp)
        sqlite3_bind_int(statement, 5, timestamp)
        
        guard sqlite3_step(statement) == SQLITE_DONE else {
            throw SQLiteError.Step(message: db.errorMessage)
        }
        
        print("dbSave success")
    }
    
    private func dbGet(key: String) -> KVStorageItem? {
        let sql = """
select key, filename, size, modification_time, last_access_time from manifest where key = ?1;
"""
        guard let statement = try? db.prepareStatement(sql: sql) else {
            return nil
        }
        
        defer {
            db.finalize(sql: statement)
        }
        
        guard sqlite3_bind_text(statement, 1, (key as NSString).utf8String, -1, nil) == SQLITE_OK else {
            return nil
        }
        
        guard sqlite3_step(statement) == SQLITE_ROW else {
            print(db.errorMessage)
            return nil
        }
        
        return dbGetItem(statement: statement)
    }
    
    private func dbGetItem(statement: OpaquePointer) -> KVStorageItem {
        var i = Int32(0)
        
        let key = sqlite3_column_text(statement, i); i += 1
        let filename = sqlite3_column_text(statement, i); i += 1
        let size = sqlite3_column_int(statement, i); i += 1
        let mtime = sqlite3_column_int(statement, i); i += 1
        let atime = sqlite3_column_int(statement, i); i += 1
        
        let item = KVStorageItem()
        item.key = key.flatMap(String.init(cString:))
        item.filename = filename.flatMap(String.init(cString:))
        item.size = Int(size)
        item.modifyTime = Double(mtime)
        item.accessTime = Double(atime)
        
        return item
    }
    
    private func dbUpdateAccessTime(key: String) throws {
        let sql = "update manifest set last_access_time = ?1 where key = ?2;"
        
        let statement = try db.prepareStatement(sql: sql)
        defer {
            db.finalize(sql: statement)
        }
        
        let timestamp = Int32(time(nil))
        
        sqlite3_bind_int(statement, 1, timestamp)
        sqlite3_bind_text(statement, 2, (key as NSString).utf8String, -1, nil)
        
        guard sqlite3_step(statement) == SQLITE_DONE else {
            throw SQLiteError.Step(message: db.errorMessage)
        }
    }
    
    private func dbDeleteItem(key: String) throws {
        let sql = "delete from manifest where key = ?1;"
        let statement = try db.prepareStatement(sql: sql)
        defer {
            db.finalize(sql: statement)
        }
        
        sqlite3_bind_text(statement, 1, (key as NSString).utf8String, -1, nil)
        
        guard sqlite3_step(statement) == SQLITE_DONE else {
            throw SQLiteError.Step(message: db.errorMessage)
        }
    }
    
    // MARK: - File -
    private func fileWrite(filename: String, data: Data) throws {
        let url = dataURL.appendingPathComponent(filename)
        
        try data.write(to: url)
    }

    private func fileDelete(filename: String) throws {
        let url = dataURL.appendingPathComponent(filename)
        
        try FileManager.default.removeItem(at: url)
    }
    
    private func fileRead(filename: String) throws -> Data {
        let url = dataURL.appendingPathComponent(filename)
        
        return try Data(contentsOf: url)
    }
    
    // MARK: - Private -
    static func MD5(string: String) -> Data {
        let length = Int(CC_MD5_DIGEST_LENGTH)
        let messageData = string.data(using:.utf8)!
        var digestData = Data(count: length)
        
        _ = digestData.withUnsafeMutableBytes { digestBytes -> UInt8 in
            messageData.withUnsafeBytes { messageBytes -> UInt8 in
                if let messageBytesBaseAddress = messageBytes.baseAddress, let digestBytesBlindMemory = digestBytes.bindMemory(to: UInt8.self).baseAddress {
                    let messageLength = CC_LONG(messageData.count)
                    CC_MD5(messageBytesBaseAddress, messageLength, digestBytesBlindMemory)
                }
                return 0
            }
        }
        return digestData
    }

    static func MD5Hex(string: String) -> String {
         return MD5(string: string).map { String(format: "%02hhx", $0) }.joined()
    }
    
    /// directory path
    let path: String
    private let dbURL: URL
    private let dataURL: URL
    private let db: SQLiteDatabase
}

extension KVStorage: SQLTable {
    static var createStatement: String {
        return """
        create table if not exists manifest (key text, filename text, size integer, modification_time integer, last_access_time integer, primary key(key)); create index if not exists last_access_time_idx on manifest(last_access_time);
        """
    }
}
