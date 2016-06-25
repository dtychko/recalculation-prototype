var accounts = [1, 2];

module.exports = {
    getAccounts() {
        return new Promise(res => {
            res(accounts.map(x => x));
        });
    },
    addIfNotExists(accountId) {
        return new Promise(res => {
            if (accounts.indexOf(accountId) === -1) {
                accounts.push(accountId);
                res(true);
            } else {
                res(false);
            }
        });
    }
};
