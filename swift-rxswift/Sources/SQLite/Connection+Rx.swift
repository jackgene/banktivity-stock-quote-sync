import RxSwift
import SQLite

extension Connection: @retroactive ReactiveCompatible {}

extension Reactive where Base: Connection {
    public func run(_ statement: String, _ bindings: Binding?...) -> Observable<Statement.Element> {
        return Observable.create { (observer: AnyObserver<Statement.Element>) in
            do {
                try self.base.run(statement, bindings).forEach { (row: Statement.Element) in
                    observer.onNext(row)
                }
            } catch {
                observer.onError(error)
            }
            observer.onCompleted()
            
            return Disposables.create()
        }
    }
}
