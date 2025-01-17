from locust import HttpUser, task, between

class MyUser(HttpUser):
    wait_time = between(1, 3)

    @task
    def invoke_microservice(self):
        self.client.get("/numericalintegralservice/0/3.14")
