const fs = require('fs');

module.exports = {
    readFile(fileName) {
        return new Promise((res, rej) => {
            fs.readFile(fileName, 'utf8', (err, data) => {
                if (err) {
                    console.error(err);
                    rej(err);
                    return;
                }

                res(data);
            });
        });
    }
};
