var redis = require('redis');
var _ = require('underscore');
var accounts = [1, 2];

module.exports = {
    getAccounts() {
        return new Promise((res) => {
            res(_.clone(accounts));
        });
    },
    addIfNotExists(accountId) {
        return new Promise(res => {
            var exists = accounts.indexOf(accountId) != -1;
            if (!exists) {
                accounts.push(accountId);
                res(true);
            } else {
                res(false);
            }
        });
    }
};




