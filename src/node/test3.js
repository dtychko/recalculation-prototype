const _ = require('underscore');
const fetch = require('node-fetch');

// const builder = ProtoBuf.loadProtoFile('./orchestrator.proto');
// const MetricSetupChangedEvent = builder.build('MetricSetupChangedEvent');
// const CalculateMetricCommand = builder.build('CalculateMetricCommand');
// const ComputationCancelledEvent = builder.build('ComputationCancelledEvent');
// const RequestQueuesCommand = builder.build('RequestQueuesCommand');
// const QueuesCollection = builder.build('QueuesCollection');
// const QueueChangedEvent = builder.build('QueueChangedEvent');

var COMPUTATION_LOG_URL = 'http://127.0.0.1:8081/update';

const eventId = '5';
const accountId = 1;
const metricSetupId = 2;
const targetIds = [1, 2, 3];

fetch(COMPUTATION_LOG_URL, {
    method: 'POST',
    body: {
        eventId,
        accountId,
        metricSetupId,
        targetIds
    }
}).then(res => {
    if (!res.ok) {
        console.log(res.status, res.statusText);
    }
    
    var json = res.json();
    console.log(` [x] Received cancellation map.`, res);
    return json;
}, err => {
    console.log(` [-] Update computation log error '${err}'`);
}).then(json => {
    console.log(json);
});
