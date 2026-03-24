select
 cat,
 sub_cat,
 CAST(BookingText AS VARCHAR) as BookingText,
 CAST(NameOriginatorBeneficiary AS VARCHAR) as NameOriginatorBeneficiary
from {{ source("raw_data", "categories") }}
