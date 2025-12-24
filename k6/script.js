import http from 'k6/http';
import { sleep } from 'k6';

export let options = {
  vus: 50,
  duration: '20s'
};

export default function () {
  const payload = JSON.stringify({
    topic: "k6",
    event_id: `${__VU}-${__ITER}`,
    timestamp: "2025-01-01T00:00:00Z",
    source: "k6",
    payload: { x: Math.random() }
  });

  http.post("http://localhost:8080/publish", payload, {
    headers: { "Content-Type": "application/json" }
  });

  sleep(0.01);
}
