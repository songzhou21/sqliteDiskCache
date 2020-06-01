//
//  ImageDiskCache.swift
//  SQLImageCache
//
//  Created by songzhou on 2020/5/29.
//  Copyright © 2020 songzhou. All rights reserved.
//

import Foundation

class ImageDiskCache: NSObject {
    init(name: String) {
        let cacheFolder = try? FileManager.default.url(for: .documentDirectory,
                                                       in: .userDomainMask,
                                                       appropriateFor: nil,
                                                       create: true)
        
        let path = cacheFolder!.appendingPathComponent(name)
        self.path = path.relativePath

        self.storage = KVStorage(path: self.path)!
        super.init()
    }
   
    @discardableResult func set(data: Data, forKey key: String) -> Bool {
        let filename = KVStorage.MD5Hex(string: key)
        return storage.saveItem(key: key, value: data, filename: filename)
    }
    
    func get(key: String) -> KVStorageItem? {
        return storage.getItem(key: key)
    }
    
    let path: String
    private let storage: KVStorage
}
