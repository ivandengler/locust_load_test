# Built-in/Generic Imports
import gevent
import grpc
import random
import time
import json
import grpc_user # type: ignore

# Libs
from locust import task, constant
from users_data import users
from google.protobuf.json_format import MessageToJson 

# Protos
from proto import auth_service_pb2, auth_service_pb2_grpc
from proto import vacancy_service_pb2, vacancy_service_pb2_grpc

# Constants
VACANCIES_URL = 'vacancies.cyrextech.net:7823'
TOTAL_USERS = 3
TIME_TO_SLEEP_MAIN_LOOP = 30
TIME_TO_SLEEP_GET_VACANCIES = 45


# Class definitions
class VacanciesLoadTestingUser(grpc_user.GrpcUser):
    wait_time = constant(TIME_TO_SLEEP_MAIN_LOOP)
    host = VACANCIES_URL
    stub_class = vacancy_service_pb2_grpc.VacancyServiceStub

    @task
    def crud_vacancies_load_test(self):
        vacancy_id = self.grpc_create_and_upload_vacancy()
        self.grpc_update_vacancy(vacancy_id)
        self.grpc_get_vacancy(vacancy_id)
        self.grpc_delete_vacancy(vacancy_id)
        self.grpc_get_all_vacancies()

    def grpc_login_and_return_access_token(self, user):
        with grpc.insecure_channel(VACANCIES_URL) as channel:
            auth_stub = auth_service_pb2_grpc.AuthServiceStub(channel)
            auth_request = auth_service_pb2.rpc__signin__user__pb2.SignInUserInput(email=user['email'], password=user['password'])
            auth_reply = auth_stub.SignInUser(auth_request)
            return json.loads(MessageToJson(auth_reply))['accessToken']

    def grpc_create_and_upload_vacancy(self):
        vacancy_request = vacancy_service_pb2.rpc__create__vacancy__pb2.CreateVacancyRequest(Title='aNewTestTitle ' + str(time.time()), Description='aNewTestDescription ' + str(time.time()), Country="ARG", Division=0)
        return json.loads(MessageToJson(self.stub.CreateVacancy(vacancy_request)))['vacancy']['Id']

    def grpc_update_vacancy(self, vacancy_id):
        vacancy_update_request = vacancy_service_pb2.rpc__update__vacancy__pb2.UpdateVacancyRequest(Id=vacancy_id, Title='Awesome new title ' + str(time.time()))
        return self.stub.UpdateVacancy(vacancy_update_request)

    def grpc_get_vacancy(self, vacancy_id):
        get_vacancy_request = vacancy_service_pb2.VacancyRequest(Id=vacancy_id)
        return self.stub.GetVacancy(get_vacancy_request)

    def grpc_delete_vacancy(self, vacancy_id):
        delete_vacancy_request = vacancy_service_pb2.VacancyRequest(Id=vacancy_id)
        return self.stub.DeleteVacancy(delete_vacancy_request)

    def grpc_get_all_vacancies(self):
        all_vacancies_request = vacancy_service_pb2.vacancy__pb2.Vacancy()
        self.stub.GetVacancies(all_vacancies_request)

    def on_start(self):
        gevent.spawn(self._on_background)
        user = users[random.randint(0, TOTAL_USERS - 1)]
        self.grpc_login_and_return_access_token(user)

    def _on_background(self):
        while True:
            gevent.sleep(TIME_TO_SLEEP_GET_VACANCIES)
            self.grpc_get_all_vacancies()
