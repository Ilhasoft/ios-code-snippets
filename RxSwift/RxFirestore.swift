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

    /**
    Reads the document referred to by this DocumentReference
     */
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

    /**
    Writes to the document referred to by this DocumentReference. If the document does not exist yet, it will be
    created.
     */
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

    /**
    Writes to the document referred to by this DocumentReference. If the document does not exist yet, it will be
    created. If you pass options, the provided data can be merged into the existing document.
     */
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

    /**
    Updates fields in the document referred to by this DocumentReference. The update will fail if applied to a
    document that does not exist.
     */
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

    /**
    Deletes the document referred to by this DocumentReference.
     */
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

    /**
    Attaches a listener for DocumentSnapshot events. When the Observable is disposed, the listener is automatically
    removed and no further action is necessary.

    NOTE: Although an onCompletion callback can be provided, it will never be called because the snapshot stream is
    never-ending.
     */
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

    /**
    Adds a new document to this collection with the specified data, assigning it a document ID automatically.
     */
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

    /**
    Executes the query and returns the results as a QuerySnapshot. When the Observable is disposed, the listener is automatically
    removed and no further action is necessary.

    NOTE: Although an onCompletion callback can be provided, it will never be called because the snapshot stream is
    never-ending.
     */
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

    /**
    Attaches a listener for QuerySnapshot events.
     */
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

    /**
    Commits all of the writes in this write batch as a single atomic unit.
     */
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

    /**
    Executes the given lambda function and then attempts to commit the changes applied within the transaction. If any
    document read within the transaction has changed, Cloud Firestore retries the lambda function. If it fails to
    commit after 5 attempts, the transaction fails.
     */
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
