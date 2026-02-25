from locust import HttpUser, between, task
from datetime import date, timedelta
import random


class ClickstreamApiUser(HttpUser):
    wait_time = between(0.2, 1.2)

    def on_start(self):
        self.today = date.today()
        self.start_date = self.today - timedelta(days=7)

    @task(4)
    def health(self):
        self.client.get("/health", name="/health")

    @task(4)
    def anomalies_recent(self):
        params = {
            "limit": random.choice([50, 100, 200]),
            "min_score": random.choice([None, 1.1, 1.3]),
        }
        self.client.get("/anomalies", params=params, name="/anomalies")

    @task(3)
    def anomaly_types(self):
        self.client.get("/anomalies/types", name="/anomalies/types")

    @task(2)
    def anomaly_timeline(self):
        self.client.get(
            "/anomalies/timeline",
            params={"hours": random.choice([6, 12, 24, 48])},
            name="/anomalies/timeline",
        )

    @task(3)
    def metrics_daily_range(self):
        params = {
            "start_date": self.start_date.isoformat(),
            "end_date": self.today.isoformat(),
        }
        self.client.get("/metrics/daily", params=params, name="/metrics/daily")

    @task(2)
    def metrics_summary(self):
        self.client.get(
            "/metrics/summary",
            params={"days": random.choice([7, 14, 30])},
            name="/metrics/summary",
        )

    @task(2)
    def items_trending(self):
        self.client.get(
            "/items/trending",
            params={"days": random.choice([7, 14]), "limit": random.choice([20, 50])},
            name="/items/trending",
        )
