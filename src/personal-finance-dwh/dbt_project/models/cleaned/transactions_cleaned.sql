select
    CreditDebit,
    CAST(Date AS DATE) as Date,
    --extract('year' FROM CAST(Date AS DATE) ) as year
    --extract('month' FROM CAST(Date AS DATE) ) as month
    --extract('day' FROM CAST(Date AS DATE) ) as day
    CAST(ValueDate AS DATE) as ValueDate,
    BookingText,
    AdditionalInfoBooking,
    NameOriginatorBeneficiary,
    AddressOriginatorBeneficiary,
    AccountBank,
    MessageReference,
    AdditionalInfoTransaction,
    Amount,
    Balance,
    case 
        when CreditDebit = 'Credit' then Amount
        else -Amount
    end as AdjustedAmount
from {{ source("raw_data", "transactions") }}
