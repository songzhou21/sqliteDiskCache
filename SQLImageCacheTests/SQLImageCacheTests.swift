//
//  SQLImageCacheTests.swift
//  SQLImageCacheTests
//
//  Created by songzhou on 2020/5/29.
//  Copyright © 2020 songzhou. All rights reserved.
//

import XCTest
@testable import SQLImageCache

class SQLImageCacheTests: XCTestCase {
    lazy var cache = ImageDiskCache(name: "com.songzhou.cache")
    
    lazy var img = UIImage(named: "1")!
    
    override func setUpWithError() throws {
        print("\(cache.path)")
    }

    override func tearDownWithError() throws {
        // Put teardown code here. This method is called after the invocation of each test method in the class.
    }

    func testSave() throws {
        let data = img.pngData()!

        cache.set(data: data, forKey: "test")
    }
    
    func testGet() throws {
        let item = cache.get(key: "test")!
        let data = item.value!
        
        let img = UIImage(data: data)
        
        XCTAssert(item.key == "test")
        XCTAssert(img != nil)
    }

}
