# -*- coding: utf-8 -*-
# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import proto  # type: ignore


__protobuf__ = proto.module(
    package="airflow.providers.google_vendor.googleads.v12.enums",
    marshal="google.ads.googleads.v12",
    manifest={"LeadFormFieldUserInputTypeEnum",},
)


class LeadFormFieldUserInputTypeEnum(proto.Message):
    r"""Describes the input type of a lead form field.
    """

    class LeadFormFieldUserInputType(proto.Enum):
        r"""Enum describing the input type of a lead form field."""
        UNSPECIFIED = 0
        UNKNOWN = 1
        FULL_NAME = 2
        EMAIL = 3
        PHONE_NUMBER = 4
        POSTAL_CODE = 5
        STREET_ADDRESS = 8
        CITY = 9
        REGION = 10
        COUNTRY = 11
        WORK_EMAIL = 12
        COMPANY_NAME = 13
        WORK_PHONE = 14
        JOB_TITLE = 15
        GOVERNMENT_ISSUED_ID_CPF_BR = 16
        GOVERNMENT_ISSUED_ID_DNI_AR = 17
        GOVERNMENT_ISSUED_ID_DNI_PE = 18
        GOVERNMENT_ISSUED_ID_RUT_CL = 19
        GOVERNMENT_ISSUED_ID_CC_CO = 20
        GOVERNMENT_ISSUED_ID_CI_EC = 21
        GOVERNMENT_ISSUED_ID_RFC_MX = 22
        FIRST_NAME = 23
        LAST_NAME = 24
        VEHICLE_MODEL = 1001
        VEHICLE_TYPE = 1002
        PREFERRED_DEALERSHIP = 1003
        VEHICLE_PURCHASE_TIMELINE = 1004
        VEHICLE_OWNERSHIP = 1005
        VEHICLE_PAYMENT_TYPE = 1009
        VEHICLE_CONDITION = 1010
        COMPANY_SIZE = 1006
        ANNUAL_SALES = 1007
        YEARS_IN_BUSINESS = 1008
        JOB_DEPARTMENT = 1011
        JOB_ROLE = 1012
        OVER_18_AGE = 1078
        OVER_19_AGE = 1079
        OVER_20_AGE = 1080
        OVER_21_AGE = 1081
        OVER_22_AGE = 1082
        OVER_23_AGE = 1083
        OVER_24_AGE = 1084
        OVER_25_AGE = 1085
        OVER_26_AGE = 1086
        OVER_27_AGE = 1087
        OVER_28_AGE = 1088
        OVER_29_AGE = 1089
        OVER_30_AGE = 1090
        OVER_31_AGE = 1091
        OVER_32_AGE = 1092
        OVER_33_AGE = 1093
        OVER_34_AGE = 1094
        OVER_35_AGE = 1095
        OVER_36_AGE = 1096
        OVER_37_AGE = 1097
        OVER_38_AGE = 1098
        OVER_39_AGE = 1099
        OVER_40_AGE = 1100
        OVER_41_AGE = 1101
        OVER_42_AGE = 1102
        OVER_43_AGE = 1103
        OVER_44_AGE = 1104
        OVER_45_AGE = 1105
        OVER_46_AGE = 1106
        OVER_47_AGE = 1107
        OVER_48_AGE = 1108
        OVER_49_AGE = 1109
        OVER_50_AGE = 1110
        OVER_51_AGE = 1111
        OVER_52_AGE = 1112
        OVER_53_AGE = 1113
        OVER_54_AGE = 1114
        OVER_55_AGE = 1115
        OVER_56_AGE = 1116
        OVER_57_AGE = 1117
        OVER_58_AGE = 1118
        OVER_59_AGE = 1119
        OVER_60_AGE = 1120
        OVER_61_AGE = 1121
        OVER_62_AGE = 1122
        OVER_63_AGE = 1123
        OVER_64_AGE = 1124
        OVER_65_AGE = 1125
        EDUCATION_PROGRAM = 1013
        EDUCATION_COURSE = 1014
        PRODUCT = 1016
        SERVICE = 1017
        OFFER = 1018
        CATEGORY = 1019
        PREFERRED_CONTACT_METHOD = 1020
        PREFERRED_LOCATION = 1021
        PREFERRED_CONTACT_TIME = 1022
        PURCHASE_TIMELINE = 1023
        YEARS_OF_EXPERIENCE = 1048
        JOB_INDUSTRY = 1049
        LEVEL_OF_EDUCATION = 1050
        PROPERTY_TYPE = 1024
        REALTOR_HELP_GOAL = 1025
        PROPERTY_COMMUNITY = 1026
        PRICE_RANGE = 1027
        NUMBER_OF_BEDROOMS = 1028
        FURNISHED_PROPERTY = 1029
        PETS_ALLOWED_PROPERTY = 1030
        NEXT_PLANNED_PURCHASE = 1031
        EVENT_SIGNUP_INTEREST = 1033
        PREFERRED_SHOPPING_PLACES = 1034
        FAVORITE_BRAND = 1035
        TRANSPORTATION_COMMERCIAL_LICENSE_TYPE = 1036
        EVENT_BOOKING_INTEREST = 1038
        DESTINATION_COUNTRY = 1039
        DESTINATION_CITY = 1040
        DEPARTURE_COUNTRY = 1041
        DEPARTURE_CITY = 1042
        DEPARTURE_DATE = 1043
        RETURN_DATE = 1044
        NUMBER_OF_TRAVELERS = 1045
        TRAVEL_BUDGET = 1046
        TRAVEL_ACCOMMODATION = 1047


__all__ = tuple(sorted(__protobuf__.manifest))
