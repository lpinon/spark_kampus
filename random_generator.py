import datetime
from random import choices, choice
from faker import Faker

Faker.seed(0)

# English Spanish French German Chinese Italian
population_languages = [1, 2, 3, 4, 5, 6]
languages_weights = [0.4, 0.3, 0.1, 0.05, 0.1, 0.05]

population_locations = [
    969, 239, 164,  # Valencia Madrid Barcelona
    34, 709, 849,  # Paris Nice Toulouse
    35, 295, 451,  # London Manchester Newcastle
    13, 28, 50, 94,  # NewYork LosAngeles Chicago Miami
    216, 499,  # Berlin Munich
    281, 537,  # Rome Milan
    6, 11, 18  # Shanghai Beijing Shenzhen
]
locations_languages = {
    1: [35, 295, 451, 13, 28, 50, 94],
    2: [969, 239, 164],
    3: [34, 709, 849],
    4: [216, 499],
    5: [6, 11, 18],
    6: [281, 537]
}

location_countries = {
    156: [969, 239, 164],
    218: [34, 709, 849],
    84: [35, 295, 451],
    54: [13, 28, 50, 94],
    200: [216, 499],
    204: [281, 537],
    110: [6, 11, 18]
}

population_categories = ["HUMOR", "TECHNOLOGY", "GAMING", "COOKING", "NEWS", "DOCUMENTARY", "MUSIC"]
categories_weights = [0.25, 0.1, 0.2, 0.1, 0.05, 0.05, 0.25]

population_influencer = [20, 50, 100, 200]
influencer_weights = [0.5, 0.3, 0.15, 0.05]


def generate_random_languages(amount=10 ** 6):
    return choices(population_languages, languages_weights, k=amount)


def generate_random_videos(amount_users=10 ** 6):
    fake_generator = Faker()
    influencer_amounts = choices(population_influencer, influencer_weights, k=amount_users+10)
    videos = []
    for user_id in range(1, amount_users + 1):
        if user_id % 2 == 0:  # Only half of users upload a video
            possible_video_amounts = [influencer_amounts[user_id] + el for el in range(-15, 20)]
            number_of_videos = choice(possible_video_amounts)
            video_languages = choices(population_languages, languages_weights, k=number_of_videos)
            video_categories = choices(population_categories, categories_weights, k=number_of_videos)
            for n_video in range(number_of_videos):  # Uploaded this amount of videos
                new_video = {
                    "id_user": user_id,
                    "title": fake_generator.sentence(nb_words=6),
                    "id_language": video_languages[n_video],
                    "category": video_categories[n_video]
                }
                videos.append(new_video)
    return videos


def generate_random_users(amount=10 ** 6):
    fake_generator = Faker()
    profiles = []
    # Half of population young
    young_amount = amount // 2
    base_year = 2000
    possible_years = [base_year + el for el in range(-10, 11)]
    chosen_years = choices(possible_years, k=young_amount)
    for i in range(young_amount):
        fake_profile = fake_generator.simple_profile()
        profiles.append({
            "username": fake_profile["username"],
            "email": fake_profile["mail"],
            "gender": fake_profile["sex"],
            "phone_number": fake_generator.phone_number(),
            "birth_date": datetime.datetime(year=chosen_years[i], month=6, day=1)
        })
    # Forth of population old
    old_amount = young_amount // 2
    base_year = 1975
    possible_years = [base_year + el for el in range(-10, 11)]
    chosen_years = choices(possible_years, k=old_amount)
    for i in range(old_amount):
        fake_profile = fake_generator.simple_profile()
        profiles.append({
            "username": fake_profile["username"],
            "email": fake_profile["mail"],
            "gender": fake_profile["sex"],
            "phone_number": fake_generator.phone_number(),
            "birth_date": datetime.datetime(year=chosen_years[i], month=6, day=1)
        })
    # Forth of population old
    very_old_amount = young_amount // 2
    base_year = 1960
    possible_years = [base_year + el for el in range(-10, 11)]
    chosen_years = choices(possible_years, k=very_old_amount)
    for i in range(very_old_amount):
        fake_profile = fake_generator.simple_profile()
        profiles.append({
            "username": fake_profile["username"],
            "email": fake_profile["mail"],
            "gender": fake_profile["sex"],
            "phone_number": fake_generator.phone_number(),
            "birth_date": datetime.datetime(year=chosen_years[i], month=6, day=1)
        })
    chosen_countries = choices([el for el in location_countries.keys()], k=len(profiles))
    # Add location to all users
    for i in range(len(profiles)):
        profiles[i]["id_country"] = chosen_countries[i]
    return profiles
