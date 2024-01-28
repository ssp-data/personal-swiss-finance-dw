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
    from transactions bt

    left join categories tc on bt.BookingText like tc.BookingText 
    left join categories ts on bt.NameOriginatorBeneficiary like ts.NameOriginatorBeneficiary
)

select
    cat,
    sub_cat,
    sum(Amount) as total_amount
from categorized_transactions
group by 1, 2 --,date