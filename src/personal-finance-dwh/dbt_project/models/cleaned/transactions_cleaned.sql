select
    CreditDebit,
    Date,
    ValueDate,
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
