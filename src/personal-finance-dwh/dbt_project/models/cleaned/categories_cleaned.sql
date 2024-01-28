select
 cat,
 sub_cat,
 BookingText,
 NameOriginatorBeneficiary
from {{ source("raw_data", "categories") }}
