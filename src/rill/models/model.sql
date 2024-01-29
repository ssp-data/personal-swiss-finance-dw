with categorized_transactions as (
    select
        bt.CreditDebit,
        CAST(strptime(Date, '%Y-%m-%d %H:%M:%S') AS DATE) as Date,
        CAST(strptime(ValueDate, '%Y-%m-%d %H:%M:%S') AS DATE) as ValueDate,
        bt.ValueDate,
        bt.BookingText,
        bt.AdditionalInfoBooking,
        bt.NameOriginatorBeneficiary,
        bt.AddressOriginatorBeneficiary,
        bt.AccountBank,
        bt.MessageReference,
        bt.AdditionalInfoTransaction,
        bt.Amount, --ROUND(RANDOM() * 1000, 2) AS Amount, 
        bt.Balance ,
        coalesce(tc.cat, coalesce(ts.cat, 'Uncategorized')) as cat,
        coalesce(tc.sub_cat, coalesce(ts.sub_cat, 'Uncategorized')) as sub_cat,
    from transactions bt

    left join manual_categories_CSV tc on bt.BookingText like tc.BookingText 
    left join manual_categories_CSV ts on bt.NameOriginatorBeneficiary like ts.NameOriginatorBeneficiary
)

select
  --extract('year' FROM date) as year,
    date,
    cat,
    sub_cat,
    Amount as total_amount --sum(Amount) as total_amount,
from categorized_transactions
--group by 
--1, --extract('year' FROM date) , 
 --2,3
