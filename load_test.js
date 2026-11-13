import http from 'k6/http';
import { check } from 'k6';

function generateUUID() {
    return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function (c) {
        const r = Math.random() * 16 | 0;
        const v = c === 'x' ? r : (r & 0x3 | 0x8);
        return v.toString(16);
    });
}

export const options = {
    scenarios: {
        burst_test: {
            executor: 'ramping-arrival-rate',
            startRate: 100,
            timeUnit: '1s',
            preAllocatedVUs: 50,
            maxVUs: 500,
            stages: [
                { duration: '10s', target: 500 },
                { duration: '20s', target: 2000 },
                { duration: '10s', target: 0 },
            ],
        },
    },
    thresholds: {
        'http_req_duration': ['p(95)<50', 'p(99)<100'],
        'http_req_failed': ['rate<0.05'],
    },
};

export default function () {
    const randomUser = `user_${Math.floor(Math.random() * 500)}`;

    const payload = JSON.stringify({
        userId: randomUser,
        channel: 'EMAIL',
        recipient: `${randomUser}@enterprise.com`,
        content: 'Your transaction was approved.',
    });

    const params = {
        headers: {
            'Content-Type': 'application/json',
            'Idempotency-Key': generateUUID(),
        },
    };

    const res = http.post('http://localhost:8080/api/v1/notify', payload, params);

    check(res, {
        'status is 202 Accepted': (r) => r.status === 202,
    });
}
