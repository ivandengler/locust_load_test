# Built-in/Generic Imports
import json
import random
import time

import grpc
from google.protobuf.json_format import MessageToJson

# Libs
from locust import task
from locust.user.task import LOCUST_STATE_STOPPING

import grpc_user  # type: ignore

# Protos
from proto import (
    auth_service_pb2,
    auth_service_pb2_grpc,
    vacancy_service_pb2,
    vacancy_service_pb2_grpc,
)
from users_data import users

# Constants
VACANCIES_URL = "vacancies.cyrextech.net:7823"
TOTAL_USERS = 3
TIME_TO_SLEEP_MAIN_LOOP = 30
TIME_TO_SLEEP_GET_VACANCIES = 45


# Class definitions
class VacanciesLoadTestingUser(grpc_user.GrpcUser):
    host = VACANCIES_URL
    stub_class = vacancy_service_pb2_grpc.VacancyServiceStub

    @task
    def get_all_vacancies(self):
        self.stub.GetVacancies(vacancy_service_pb2.vacancy__pb2)
        time.sleep(TIME_TO_SLEEP_GET_VACANCIES)

    @task(3)
    def crud_vacancies_load_test(self):
        vacancy_id = self.grpc_create_and_upload_vacancy()
        self.grpc_update_vacancy(vacancy_id)
        self.grpc_get_vacancy(vacancy_id)
        self.grpc_delete_vacancy(vacancy_id)
        time.sleep(TIME_TO_SLEEP_MAIN_LOOP)

    def grpc_login_and_return_access_token(self, user):
        with grpc.insecure_channel(VACANCIES_URL) as channel:
            auth_stub = auth_service_pb2_grpc.AuthServiceStub(channel)
            auth_request = auth_service_pb2.rpc__signin__user__pb2.SignInUserInput(
                email=user["email"], password=user["password"]
            )
            auth_reply = auth_stub.SignInUser(auth_request)
            return json.loads(MessageToJson(auth_reply))["accessToken"]

    def grpc_create_and_upload_vacancy(self):
        vacancy_request = (
            vacancy_service_pb2.rpc__create__vacancy__pb2.CreateVacancyRequest(
                Title="aNewTestTitle " + str(time.time()),
                Description="aNewTestDescription " + str(time.time()),
                Country="ARG",
                Division=0,
            )
        )
        return json.loads(MessageToJson(self.stub.CreateVacancy(vacancy_request)))[
            "vacancy"
        ]["Id"]

    def grpc_update_vacancy(self, vacancy_id):
        vacancy_update_request = (
            vacancy_service_pb2.rpc__update__vacancy__pb2.UpdateVacancyRequest(
                Id=vacancy_id, Title="Awesome new title " + str(time.time())
            )
        )
        return self.stub.UpdateVacancy(vacancy_update_request)

    def grpc_get_vacancy(self, vacancy_id):
        get_vacancy_request = vacancy_service_pb2.VacancyRequest(Id=vacancy_id)
        return self.stub.GetVacancy(get_vacancy_request)

    def grpc_delete_vacancy(self, vacancy_id):
        delete_vacancy_request = vacancy_service_pb2.VacancyRequest(Id=vacancy_id)
        return self.stub.DeleteVacancy(delete_vacancy_request)

    def on_start(self):
        user = users[random.randint(0, TOTAL_USERS - 1)]
        self.grpc_login_and_return_access_token(user)
        # gevent.spawn(self._on_background())

    def _on_background(self):
        while self.environment.running.state != LOCUST_STATE_STOPPING:
            self.stub.GetVacancies(vacancy_service_pb2.vacancy__pb2)
            time.sleep(TIME_TO_SLEEP_GET_VACANCIES)
