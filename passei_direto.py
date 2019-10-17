from luigi.contrib.external_program import ExternalProgramTask
from luigi.parameter import IntParameter, Parameter
from luigi import LocalTarget, Task

import pandas as pd
import numpy as np
import time

class TransferInformation:

    def __init__(self,df_students):
        self.df_students = df_students

    def from_cities_to_states(self):
        print("Tranferindo informações de 'City' para 'State'...")
        dict_city_state = {}
        lista_imp_city = []
        cidades = tuple(
            self.df_students["City"].value_counts().reset_index()["index"].values)

        for cidade in cidades:
            df_imp = pd.DataFrame(
                self.df_students[["State", "City"]].loc[self.df_students['City'] == cidade])
            norm_val = df_imp["State"].value_counts().reset_index()[
                "State"].sum()
            lista_val = pd.DataFrame(self.df_students[["State", "City"]].loc[self.df_students['City'] == cidade]["State"].value_counts(
            )).reset_index()["State"].apply(lambda x: round(x / norm_val, 4))
            dict_city_state[cidade] = tuple(
                zip(df_imp["State"].value_counts().reset_index()["index"], lista_val))

        for cidade in cidades:
            city_nans = self.df_students[[
                "State", "City"]].loc[self.df_students['City'] == cidade].loc[self.df_students['State'].isna()]
            population_city = list(pd.DataFrame(
                list(dict_city_state[cidade]))[0])
            distribution = list(pd.DataFrame(list(dict_city_state[cidade]))[1])
            if len(distribution) == 1:
                city_nans["State"] = population_city[0]
            else:
                city_nans["State"] = city_nans["State"].apply(lambda x: random.choices(
                    population=population_city, weights=distribution, k=1))
            lista_imp_city.append(city_nans)

        imp_city = pd.concat(lista_imp_city)
        imp_city["State"] = imp_city["State"].apply(
            lambda x: str(x).strip("[").strip("]").strip("'"))

        idx = imp_city.index.intersection(self.df_students.index)
        cols = imp_city.columns.intersection(self.df_students.columns)

        self.df_students = imp_city.loc[idx, cols].combine_first(self.df_students)
        print("Terminado.\n")

    def from_states_to_cities(self):
        print("Tranferindo informações de 'State' para 'City'...")
        dict_state_city = {}
        lista_imp_states = []

        estados = tuple(
            self.df_students["State"].value_counts().reset_index()["index"].values)

        for estado in estados:
            df_imp = pd.DataFrame(
                self.df_students[["State", "City"]].loc[self.df_students['State'] == estado])
            norm_val = df_imp["City"].value_counts().reset_index()[
                "City"].sum()
            lista_val = pd.DataFrame(self.df_students[["State", "City"]].loc[self.df_students['State'] == estado]["City"].value_counts(
            )).reset_index()["City"].apply(lambda x: round(x / norm_val, 4))
            dict_state_city[estado] = tuple(
                zip(df_imp["City"].value_counts().reset_index()["index"], lista_val))

        for estado in estados:
            state_nans = self.df_students[[
                "State", "City"]].loc[self.df_students['State'] == estado].loc[self.df_students['City'].isna()]
            population_state = list(pd.DataFrame(
                list(dict_state_city[estado]))[0])
            distribution = list(pd.DataFrame(list(dict_state_city[estado]))[1])

            state_nans["City"] = state_nans["City"].apply(lambda x: random.choices(
                population=population_state, weights=distribution, k=1))
            lista_imp_states.append(state_nans)

        imp_states = pd.concat(lista_imp_states)
        imp_states["City"] = imp_states["City"].apply(
            lambda x: str(x).strip("[").strip("]").strip("'"))

        idx = imp_states.index.intersection(self.df_students.index)
        cols = imp_states.columns.intersection(self.df_students.columns)

        self.df_students = imp_states.loc[idx,
                                          cols].combine_first(self.df_students)
        print("Terminado.\n")

    def from_universities_to_states(self):
        print("Tranferindo informações de 'UniversityId' para 'City'...")
        dict_univ_state = {}
        lista_imp_univ_state = []

        universidades = tuple(
            self.df_students["UniversityId"].value_counts().reset_index()["index"].values)[
            :250]

        for univ in universidades:
            df_imp = pd.DataFrame(self.df_students[[
                                  "UniversityId", "State"]].loc[self.df_students['UniversityId'] == univ])
            norm_val = df_imp["State"].value_counts().reset_index()[
                "State"].sum()

            lista_val = pd.DataFrame(self.df_students[["State", "UniversityId"]].loc[self.df_students['UniversityId'] == univ]["State"].value_counts(
            )).reset_index()["State"].apply(lambda x: round(x / norm_val, 4))
            dict_univ_state[univ] = tuple(
                zip(df_imp["State"].value_counts().reset_index()["index"], lista_val))

        for univ in universidades:
            try:
                univ_nans = self.df_students[[
                    "State", "UniversityId"]].loc[self.df_students['UniversityId'] == univ].loc[self.df_students['State'].isna()]
                population_univ = list(pd.DataFrame(
                    list(dict_univ_state[univ]))[0])
                distribution = list(pd.DataFrame(
                    list(dict_univ_state[univ]))[1])
                if len(distribution) == 1:
                    univ_nans["State"] = population_univ[0]
                else:
                    univ_nans["State"] = univ_nans["State"].apply(lambda x: random.choices(
                        population=population_univ, weights=distribution, k=1))
                lista_imp_univ_state.append(univ_nans)
            except BaseException:
                pass

        imp_univ = pd.concat(lista_imp_univ_state)
        imp_univ["State"] = imp_univ["State"].apply(
            lambda x: str(x).strip("[").strip("]").strip("'"))

        idx = imp_univ.index.intersection(self.df_students.index)
        cols = imp_univ.columns.intersection(self.df_students.columns)

        self.df_students = imp_univ.loc[idx,
                                        cols].combine_first(self.df_students)
        print("Terminado.\n")

    def imputar_dados(self,full_cycle=1):
        self.df_students = df_students
        for i in range(full_cycle):
            self.from_states_to_cities()
            self.from_cities_to_states()
            self.from_universities_to_states()
            self.from_states_to_cities()
        return self.df_students


class DataRefactor:


    def __init__(self, df_students, df_sfs, df_subscriptions, df_sessions, df_subjects, df_courses, df_universities):
        self.df_sessions = df_sessions
        self.df_sfs = df_sfs
        self.df_students = df_students
        self.df_subscriptions = df_subscriptions
        self.df_subjects = df_subjects
        self.df_courses = df_courses
        self.df_universities = df_universities

    def convert_to_category(self):
        #converter todas as colunas categoricas para category data type pra economizar memória
        for column in self.df_students.columns:
            self.df_students[column] = self.df_students[column].astype('category')

    def organize_data(self):
        self.df_students = self.df_students[['Id', 'CourseId', 'UniversityId', 'State', 'City', 'RegisteredDate', 'StudentClient', 'SignupSource']]
        self.df_sfs = self.df_sfs[['StudentId', 'SubjectId', 'FollowDate']]
        self.df_subscriptions = self.df_subscriptions[['StudentId', 'PlanType', 'PaymentDate']]
        self.df_sessions = self.df_sessions[['StudentId', 'SessionStartTime', 'StudentClient']]

    def create_id_to_name(self):
        for name in ("subjects","courses","universities"):
            LOC = f"""
    self.change_id_to_name_{name}
    self.change_id_to_name_{name} = self.df_{name}.set_index('Id').to_dict()['Name']
    """
            exec(LOC)
    
    def replace_all_id_to_name(self):
        for name in ("subjects","courses","universities"):
            if name == "subjects":
                self.df_sfs.SubjectId.replace(self.change_id_to_name_subjects, inplace = True)
            elif name == "courses":
                self.df_students.CourseId.replace(self.change_id_to_name_courses, inplace = True)
            elif name == "universities":
                self.df_students.UniversityId.replace(self.change_id_to_name_universities,inplace = True)
    
    def time_tuple_conversion(self, refactor_data):
        # para análise de séries temporais, pode ser que ajude, não sei o padrão de uso
        for date in ("RegisteredDate", "PaymentDate", "FollowDate", "SessionStartTime"):
            try:
                for df in refactor_data:
                    if date in df.columns:
                        df[date] = df[date].apply(lambda x: x[:19]).apply(time.strptime, args=["%Y-%m-%d %H:%M:%S"]).apply(lambda x: x[:6])
            except:
                print(f"Provavelmente já foi convertida a coluna {date}")
                pass

    def export(self):
        pass
    
    def execute(self):
        refactor_data = [self.df_students, self.df_sfs, self.df_subscriptions, self.df_sessions, self.df_subjects, self.df_courses, self.df_universities]
        self.time_tuple_conversion(refactor_data)
        self.create_id_to_name()
        self.replace_all_id_to_name()
        self.organize_data()
        refactored_data = [self.df_students, self.df_sfs, self.df_subscriptions, self.df_sessions, self.df_subjects, self.df_courses, self.df_universities]
        return refactored_data
        

class CreateNewDatabases:

    def __init__(self, df_students, df_sfs, df_subscriptions, df_sessions, df_subjects, df_courses, df_universities):
        self.df_students = df_students
        self.df_sfs = df_sfs
        self.df_subscriptions = df_subscriptions
        self.df_sessions = df_sessions
        self.df_subjects = df_subjects
        self.df_courses = df_courses
        self.df_universities = df_universities

    def create_new_bases(self):

        self.students_subjects = self.df_sfs.rename(columns = {"StudentId":"Id"}).merge(self.df_students, on= ["Id"]).rename(
            columns = {"Id":"StudentId", "SubjectId":"SubjectName", "CourseId": "CourseName",
                       "UniversityId":"UniversityName" })[
        ['StudentId', 'CourseName', 'UniversityName', 'RegisteredDate', 'State', 'City', 'SubjectName', 'FollowDate', 'StudentClient', 'SignupSource']];

        self.students_subjects_subscriptions = self.df_sfs.rename(columns = {"StudentId":"Id"}).merge(self.df_students, on= ["Id"]).rename(
            columns = {"Id":"StudentId", "SubjectId":"SubjectName", "CourseId": "CourseName",
                       "UniversityId":"UniversityName" })[
        ['StudentId', 'CourseName', 'UniversityName', 'RegisteredDate', 'State', 'City', 'SubjectName', 'FollowDate', 'StudentClient', 'SignupSource']].merge(self.df_subscriptions, on = ["StudentId"]);

        self.students_sessions = self.df_sessions.rename(columns = {"StudentId":"Id"}).merge(self.df_students, on = ["Id"]).rename(
            columns = {"Id":"StudentId","CourseId":"CourseName","UniversityId":"UniversityName"})[['StudentId','CourseName','UniversityName','SessionStartTime','StudentClient_x','StudentClient_y','State','City','RegisteredDate','SignupSource']];

        self.sessions_subscriptions = self.df_sessions.merge(self.df_subscriptions, on= ["StudentId"])[['StudentId','SessionStartTime','StudentClient','PaymentDate', 'PlanType']];

        self.students_subscriptions = self.df_subscriptions.rename(columns = {"StudentId":"Id"}).merge(self.df_students, on= ["Id"]).rename(
            columns = {"Id":"StudentId", "CourseId": "CourseName",
                       "UniversityId":"UniversityName" })[['StudentId', 'UniversityName', 'CourseName', 'PlanType', 'PaymentDate', 'State', 'City', 'RegisteredDate', 'SignupSource', 'StudentClient']];

        database = [self.students_subjects, self.students_subjects_subscriptions, self.students_sessions, self.sessions_subscriptions, self.students_subscriptions]
        DataRefactor.time_tuple_conversion(database)
    
    def export(self):

        new_path = f"banco_bi/database/{time_string}"
        time_string = time.asctime().replace(" ", "_")
        time_string = time_string[20:24] +"_"+ time_string[4:7] +"_"+ time_string[8:10]
        ssj = f"{new_path}\students_subjects_{time_string}.csv"
        ssjsub = f"{new_path}\students_subjects_subscriptions_{time_string}.csv"
        sse = f"{new_path}\students_sessions_{time_string}.csv"
        sesub =  f"{new_path}\sessions_subscriptions_{time_string}.csv"
        ssub = f"{new_path}\students_subscriptions_{time_string}.csv"

        try:
            os.makedirs(new_path)

            self.students_subjects.to_csv(ssj)
            self.students_subjects_subscriptions.to_csv(ssjsub)
            self.students_sessions.to_csv(sse)
            self.sessions_subscriptions.to_csv(sesub)
            self.students_subscriptions.to_csv(ssub)

        except FileExistsError:

            self.students_subjects.to_csv(ssj)
            self.students_subjects_subscriptions.to_csv(ssjsub)
            self.students_sessions.to_csv(sse)
            self.sessions_subscriptions.to_csv(sesub)
            self.students_subscriptions.to_csv(ssub)


    def run():
        pass

class VisualizeData:
    def __init__(self):
        pass


class FetchJsonData:

    def __init__(self):
        self.df_courses = pd.read_json('datasets/courses.json', orient='records')
        self.df_sessions = pd.read_json('datasets/sessions.json', orient='records')
        self.df_sfs = pd.read_json('datasets/student_follow_subject.json', orient='records')
        self.df_students = pd.read_json('datasets/students.json', orient='records')
        self.df_subjects = pd.read_json('datasets/subjects.json', orient='records')
        self.df_subscriptions = pd.read_json('datasets/subscriptions.json', orient='records')
        self.df_universities = pd.read_json('datasets/universities.json', orient='records')
        self.lista = [self.df_students, self.df_sfs, self.df_subscriptions, self.df_sessions, self.df_subjects, self.df_courses, self.df_universities]

    def gathering_data(self):
        return self.lista