var _ = require('underscore');

var store = {};

function getKeyGenerator(accountId, metricSetupId) {
    return targetId => `${accountId}_${metricSetupId}_${targetId}`;
}

module.exports = {
    add(accountId, metricSetupId, targetIds, eventId) {
        var generateKey = getKeyGenerator(accountId, metricSetupId);

        _.each(targetIds, targetId => {
            var key = generateKey(targetId);
            var eventIds = store[key] = store[key] || [];
            if (eventIds.indexOf(eventId) === -1) {
                eventIds.push(eventId);
            }
        })
    },

    has(accountId, metricSetupId, targetId, eventId) {
        var key = getKeyGenerator(accountId, metricSetupId)(targetId);
        var eventIds = store[key];

        return eventIds && eventIds.indexOf(eventId) !== -1;
    },

    remove(accountId, metricSetupId, targetId, eventId) {
        var key = getKeyGenerator(accountId, metricSetupId)(targetId);
        var eventIds = store[key];

        if (!eventIds) {
            return;
        }

        var index = eventIds.indexOf(eventId);

        if (index !== -1) {
            eventIds.splice(index, 1);
        }
    }
};
