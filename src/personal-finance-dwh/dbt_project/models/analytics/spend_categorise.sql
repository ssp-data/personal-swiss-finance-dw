
with categorized_transactions as (
    select
        bt.CreditDebit,
        bt.Date,
        bt.ValueDate,
        bt.BookingText,
        bt.AdditionalInfoBooking,
        bt.NameOriginatorBeneficiary,
        bt.AddressOriginatorBeneficiary,
        bt.AccountBank,
        bt.MessageReference,
        bt.AdditionalInfoTransaction,
        bt.Amount,
        bt.Balance,
        coalesce(tc.cat, coalesce(ts.cat, 'Uncategorized')) as cat,
        coalesce(tc.sub_cat, coalesce(ts.sub_cat, 'Uncategorized')) as sub_cat,
    from {{ ref("transactions_cleaned") }} bt
    left join {{ ref("categories_cleaned") }} tc on lower(bt.BookingText) like lower(tc.BookingText)
    left join {{ ref("categories_cleaned") }} ts on lower(bt.NameOriginatorBeneficiary) like lower(ts.NameOriginatorBeneficiary)
)

select
    cat,
    sub_cat,
    sum(Amount) as total_amount
from categorized_transactions
group by 1, 2


