import Firebase
import RxSwift

struct FirestoreError {
    static let batchSizeExceded = NSError(
        domain: FirestoreErrorDomain,
        code: 17, // Error codes upto 16 are default error codes
        userInfo: [NSLocalizedDescriptionKey: "Batch size should be less than 500"]
    )
}

extension Reactive where Base: DocumentReference {

    func getDocument() -> Single<DocumentSnapshot> {
        return Single.create { observer in
            let disposable = Disposables.create { }
            self.base.getDocument(completion: { (snapshot, error) in
                guard !disposable.isDisposed else { return }
                if let snapshot = snapshot {
                    observer(.success(snapshot))
                } else if let error = error {
                    observer(.error(error))
                }
            })
            return disposable
        }
    }

    func set(documentData: [String: Any]) -> Completable {
        return Completable.create { observer in
            let disposable = Disposables.create { }
            self.base.setData(documentData, completion: { error in
                guard !disposable.isDisposed else { return }
                if let error = error {
                    observer(.error(error))
                    return
                }
                observer(.completed)
            })
            return disposable
        }
    }

    func set(documentData: [String: Any], options: SetOptions) -> Completable {
        return Completable.create { observer in
            let disposable = Disposables.create { }
            self.base.setData(documentData, options: options, completion: { error in
                guard !disposable.isDisposed else { return }
                if let error = error {
                    observer(.error(error))
                    return
                }
                observer(.completed)
            })
            return disposable
        }
    }

    func update(documentData: [String: Any]) -> Completable {
        return Completable.create { observer in
            let disposable = Disposables.create { }
            self.base.updateData(documentData, completion: { error in
                guard !disposable.isDisposed else { return }
                if let error = error {
                    observer(.error(error))
                    return
                }
                observer(.completed)
            })
            return disposable
        }
    }

    func delete() -> Completable {
        return Completable.create { observer in
            let disposable = Disposables.create { }
            self.base.delete(completion: { error in
                guard !disposable.isDisposed else { return }
                if let error = error {
                    observer(.error(error))
                    return
                }
                observer(.completed)
            })
            return disposable
        }
    }

    func listen(with options: DocumentListenOptions? = nil) -> Observable<DocumentSnapshot> {
        return Observable.create { observer in
            var listener: ListenerRegistration?
            let disposable = Disposables.create { listener?.remove() }
            listener = self.base.addSnapshotListener(options: options, listener: { (snapshot, error) in
                guard !disposable.isDisposed else { return }
                if let snapshot = snapshot {
                    observer.onNext(snapshot)
                } else if let error = error {
                    observer.onError(error)
                }
            })
            return disposable
        }
    }
}

extension Reactive where Base: CollectionReference {

    func add(document: [String: Any]) -> Single<DocumentReference> {
        return Single.create { observer in
            let disposable = Disposables.create { }
            var reference: DocumentReference?
            reference = self.base.addDocument(data: document) { error in
                guard !disposable.isDisposed else { return }
                if let error = error {
                    observer(.error(error))
                } else if let reference = reference {
                    observer(.success(reference))
                }
            }
            return disposable
        }
    }

    func deleteAll(_ batchLimit: Int = 100) -> Completable {
        return Completable.create { observer in
            let disposable = Disposables.create { }
            if batchLimit > 500 { observer(.error(FirestoreError.batchSizeExceded)) }
            self.base.limit(to: batchLimit).getDocuments(completion: { (snapshot, error) in
                guard !disposable.isDisposed else { return }
                if let error = error {
                    observer(.error(error))
                    return
                }
                guard let snapshot = snapshot, !snapshot.isEmpty else {
                    observer(.completed)
                    return
                }
                let batch = self.base.firestore.batch()
                snapshot.documents.forEach { batch.deleteDocument($0.reference) }
                batch.commit(completion: { error in
                    if let error = error {
                        observer(.error(error))
                        return
                    }
                    observer(.completed)
                })
            })
            return disposable
        }
    }
}

extension Reactive where Base: Query {

    func getDocuments() -> Single<QuerySnapshot> {
        return Single.create { observer in
            let disposable = Disposables.create { }
            self.base.getDocuments(completion: { (snapshot, error) in
                guard !disposable.isDisposed else { return }
                if let snapshot = snapshot {
                    observer(.success(snapshot))
                } else if let error = error {
                    observer(.error(error))
                }
            })
            return disposable
        }
    }

    func listen(with options: QueryListenOptions? = nil) -> Observable<QuerySnapshot> {
        return Observable.create { observer in
            var listener: ListenerRegistration?
            let disposable = Disposables.create { listener?.remove() }
            listener = self.base.addSnapshotListener(options: options, listener: { (snapshot, error) in
                guard !disposable.isDisposed else { return }
                if let snapshot = snapshot {
                    observer.onNext(snapshot)
                } else if let error = error {
                    observer.onError(error)
                }
            })
            return disposable
        }
    }
}

extension Reactive where Base: WriteBatch {
    func commit() -> Completable {
        return Completable.create { observer in
            let disposable = Disposables.create { }
            self.base.commit(completion: { error in
                guard !disposable.isDisposed else { return }
                if let error = error {
                    observer(.error(error))
                    return
                }
                observer(.completed)
            })
            return disposable
        }
    }
}

extension Reactive where Base: Firestore {
    func runTransaction(_ updateBlock: @escaping (Transaction, NSErrorPointer) -> Any?,
                        successCompletion: ((Any) -> Void)?,
                        errorCompletion: ((Error) -> Void)?) -> Single<Any> {
        return Single.create { observer in
            let disposable = Disposables.create { }
            self.base.runTransaction(updateBlock, completion: { (object, error) in
                guard !disposable.isDisposed else { return }
                if let object = object {
                    successCompletion?(object)
                    observer(.success(object))
                } else if let error = error {
                    errorCompletion?(error)
                    observer(.error(error))
                }
            })
            return disposable
        }
    }
}
