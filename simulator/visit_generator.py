import datetime

from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from simulator.random_generator import choose_from_list, get_random_location_from_country, locations_languages, \
    get_random_video_by_category

AMOUNT_USERS = 1000000
USERS_VERY_YOUNG = AMOUNT_USERS // 2
USERS_YOUNG = USERS_VERY_YOUNG // 2
USERS_OLD = USERS_YOUNG


def generate_visit(id_user, id_video, id_device, id_location):
    return {
        "id_user": id_user,
        "id_video": id_video,
        "id_device": id_device,
        "id_location": id_location,
        "visit_date": datetime.datetime.now(),
    }


class VisitGenerator:
    def __init__(self, videos_df: DataFrame, users_df: DataFrame, devices_df: DataFrame, locations_df: DataFrame):
        self.videos = videos_df
        self.users = users_df
        self.devices = devices_df
        self.locations = locations_df
        self.possible_videos = [(el[0], el[1], el[2]) for el in self.videos.select(col("id_video"), col("id_language"), col("category")).distinct().collect()]
        self.possible_devices = [el[0] for el in self.devices.select(col("id_device")).distinct().collect()]
        self.possible_users = [(el[0], el[1], el[2]) for el in
                               self.users.select(col("id_user"), col("gender"), col("id_country")).distinct().collect()]

    def generate_random_visits(self, amount=1000):
        random_users = choose_from_list(self.possible_users, amount)
        random_videos = choose_from_list(self.possible_videos, 1000)
        random_devices = choose_from_list(self.possible_devices, amount)
        random_visits = []
        for i, random_user in enumerate(random_users):
            user_device = random_devices[i]
            user_id = random_user[0]
            user_sex = random_user[1]
            user_country = random_user[2]
            user_location = get_random_location_from_country(user_country)
            if user_id > AMOUNT_USERS - USERS_VERY_YOUNG:
                video_id = get_random_video_by_category(random_videos,  choose_from_list(["HUMOR", "TECHNOLOGY", "GAMING", "MUSIC"])[0])
            elif user_id > AMOUNT_USERS - USERS_VERY_YOUNG - USERS_YOUNG:
                video_id = get_random_video_by_category(random_videos,
                                                        choose_from_list(["HUMOR", "TECHNOLOGY", "GAMING", "COOKING", "NEWS", "DOCUMENTARY", "MUSIC"])[0])
            else:
                video_id = get_random_video_by_category(random_videos,
                                                        choose_from_list(["HUMOR", "TECHNOLOGY", "COOKING", "NEWS", "DOCUMENTARY"])[0])
            random_visits.append(generate_visit(user_id, video_id, user_device, user_location))
        return random_visits

    def generate_visits(self):
        amount_of_visits = input("Number of visits (default 1.000): ")
        amount_of_visits = int(amount_of_visits) if amount_of_visits != '' else 1000
        mode = input("Mode (R-RANDOM, C-CATEGORY, I-INFLUENCER):").upper()
        if mode not in ["RANDOM", "CATEGORY", "INFLUENCER", "R", "C", "I"]:
            raise Exception("Invalid mode")
        mode = mode if mode != '' else "RANDOM"
        print("Generating {} visits with mode {}".format(amount_of_visits, mode))
        if mode == "RANDOM" or mode == "R":
            return self.generate_random_visits(amount_of_visits)
