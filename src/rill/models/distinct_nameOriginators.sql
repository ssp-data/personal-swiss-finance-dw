select distinct MessageReference

--NameOriginatorBeneficiary, BookingText, AdditionalInfoBooking, AdditionalInfoTransaction, MessageReference --distinct NameOriginatorBeneficiary, BookingText --, amount
from transactions
--where lower(NameOriginatorBeneficiary) like '%mam%'
where amount > 100
order by amount desc