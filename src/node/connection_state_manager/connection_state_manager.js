const amqp = require('amqplib');

const states = {
    INITIAL: 'INITIAL',
    CONNECTED: 'CONNECTED',
    SUSPENDED: 'SUSPENDED'
};


class DeferredProvider {
    constructor({serverUrl, waitTimesSequence}) {
        this._waitTimesSequence = waitTimesSequence;
        this._state = states.INITIAL;
        this._target = null;
    }

    get() {
        if (this._state === states.CONNECTED) {
            return Promise.resolve(this._target);
        }

        return this._createTarget();
    }

    suspend(err) {
        this._handleError(err);
    }

    _createTarget() {
        return new Promise((res, rej) => {
            this._createTargetRecursive(res, 0);
        });
    }

    _createTargetRecursive(res, attempt) {
        return this._asyncTargetSource()
            .then(target => {
                target.on('error', err => {
                    this._handleError(err);
                });
                this._target = target;
                this._state = states.CONNECTED;
                res(target);
            }, err => {
                this._handleErrorOnCreatingTarget(res, err, attempt);
            });

    }

    _handleErrorOnCreatingTarget(res, err, attempt) {
        this._handleError(err);
        const length = this._waitTimesSequence.length;
        const timeOut = length - 1 <= attempt ?
            this._waitTimesSequence[attempt] :
            this._waitTimesSequence[length - 1];

        setTimeout(() => {
            this._createTargetRecursive(res, attempt + 1);
        }, timeOut);
    }

    _handleError(err) {
        console.log(err);
        this._state = states.SUSPENDED;
        this._target = null;
    }

    _asyncTargetSource() {
        return Promise.resolve({});
    }
}

class DeferredConnectionProvider extends DeferredProvider {
    constructor({serverUrl, waitTimesSequence}) {
        super({waitTimesSequence});
        this._serverUrl = serverUrl;
    }

    _asyncTargetSource() {
        return amqp.connect(this._serverUrl);
    }
}


class DeferredChannelProvider extends DeferredProvider {
    constructor({connectionManager, waitTimesSequence}) {
        super({waitTimesSequence});
        this._connectionManager = connectionManager;
    }

    _asyncTargetSource() {
        return this._connectionManager.get().then(conn => conn.createChannel());
    }
}

module.exports = {
    DeferredChannelProvider,
    DeferredConnectionProvider
};

