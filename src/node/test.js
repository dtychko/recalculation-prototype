// var fn = () => {
//     return new Promise((resolve, reject) => {
//         setTimeout(() => {
//             console.log('hello from promise');
//             resolve();
//         }, 1000);
//     });
// };
//
// fn()
//     .then(() => {
//         console.log('promise was resolved');
//     });

// var promise = new Promise((res, rej) => {
//     setTimeout(() => {res(0)}, 1000);
// });
//
// promise.then(() => {
//     return new Promise((res, rej) => {
//         setTimeout(() => {res(1)}, 1000);
//     });
// }).then(() => {
//     console.log('Done');
// });

// function get(key, callback) {
//     setTimeout(() => {
//         callback(key);
//     }, 100);
// }
//
// var promise = Promise.resolve({});
//
// for (let i = 0; i < 10; i++) {
//     promise = promise.then(map =>
//         new Promise((res, rej) => {
//             get(i, value => {
//                 console.log('retrived value for i = ' + i);
//
//                 if (i === 6) {
//                     rej();
//                 }
//
//                 map[i] = value;
//                 res(map);
//             });
//         }));
// }
//
// promise.then(map => {
//     console.log(JSON.stringify(map));
// }, () => {
//     console.log('error');
// });

var promise = new Promise(res => {
    setTimeout(() => {
        res([1, 2, 3]);
    }, 1);
}).then(([x1, x2, x3]) => {
    console.log(x1, x2, x3); // should be 100
    return x1;
}).then(x => {
    console.log(x);
});
