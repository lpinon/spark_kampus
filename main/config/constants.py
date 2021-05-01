# CONFIG ENTRIES
CURRENT_DATA_DELTA_TABLE_NAME = "CURRENT_DATA_DELTA_TABLE_NAME"
DELTA_SRC_PATH = "DELTA_SRC"
POSTGRESQL_DB = "POSTGRESQL_DB"
POSTGRESQL_USER = "POSTGRESQL_USER"
POSTGRESQL_PASSWORD = "POSTGRESQL_PASSWORD"
POSTGRESQL_HOST = "POSTGRESQL_HOST"
KAFKA_SERVER = "KAFKA_SERVER"

# CONFIG VALUES - in separate source
DELTA_LOCATION = "delta/"
POSTGRESQL_DB_VALUE = "postgres"
POSTGRESQL_USER_VALUE = "kampus"
POSTGRESQL_PASSWORD_VALUE = "kampus"
POSTGRESQL_HOST_VALUE = "localhost"
KAFKA_SERVER_NAME = "localhost:9092"

# Data model config

##########
# VISITS #
##########
VISITS_TABLE = "visit"
VISITS_ID = "id_visit"
VISITS_USER_ID = "id_user"
VISITS_VIDEO_ID = "id_video"
VISITS_DEVICE_ID = "id_device"
VISITS_LOCATION_ID = "id_location"
VISITS_VISIT_TIMESTAMP = "visit_date"

# DELTA model config

##################
# VISITS X VIDEO #
##################
VISITSXVIDEO_TABLE = "visits"
VISITSXVIDEO_VIDEO_ID = "id_video"
VISITSXVIDEO_COUNT = "count"
VISITSXVIDEO_PRECOMPUTED_TABLE = "precomputed.video_views"
