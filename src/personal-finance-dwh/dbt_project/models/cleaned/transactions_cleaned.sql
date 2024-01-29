select
    CreditDebit,
    CAST(strptime(Date, '%Y-%m-%d %H:%M:%S') AS DATE) as Date,
    --extract('year' FROM CAST(strptime(Date, '%Y-%m-%d %H:%M:%S') AS DATE) ) as year
    --extract('month' FROM CAST(strptime(Date, '%Y-%m-%d %H:%M:%S') AS DATE) ) as month
    --extract('day' FROM CAST(strptime(Date, '%Y-%m-%d %H:%M:%S') AS DATE) ) as day
    CAST(strptime(ValueDate, '%Y-%m-%d %H:%M:%S') AS DATE) as ValueDate,
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
