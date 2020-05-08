import XCTest

#if !canImport(ObjectiveC)
public func allTests() -> [XCTestCaseEntry] {
    return [
        testCase(banktivity_stock_quote_sync_swiftTests.allTests),
    ]
}
#endif
