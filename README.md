

# Personal Finance Data Warehouse

This project startet long ago with forking the [etl-with-airflow](https://gtoonstra.github.io/etl-with-airflow/) repo to learn Apache Airlow.

I added some features to automate my personal fincances to improve the bad state back then in swiss banking with these featues:
* to extract transaction information from an t940-export (old swiss format) export to a JSON
* startet to scrape viseca credit card information
* but ended up downloading de transaction from the Web UI with mark all and save it to a text file and automatically write that to a transactions file as well

Today (August 2021) I found that project and was motivated to migrate it to Dagster and maybe add some featues. Let's see how that goes 😉.

The hard part is, that swiss banks living in the stoneage and won't provide any API or standard interfaces, theresore I had to find other ways. Luckily Revolut and other Internet banks make it possible nowdays.
