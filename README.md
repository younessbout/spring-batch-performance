# Increase Spring Batch Performance

This repository is aimed to demonstrate spring batch performance optimization through different features:
* Multi-Threaded Steps [Medium Article](https://medium.com/@YounessBout/increase-spring-batch-performance-through-multithreading-b513ca90aeb5)
* Asynchronous Processing
* Parallel Steps
* Remote Chunking
* Remote Partitioning

For theses examples I'm generating 100M dummy data and inserting them into a dummy transanctions table.
 

```sql
DO $$
<<block>>
declare
counter     integer := 0;
rec         RECORD;

begin
  --
  for rec in (
    with nums as (
      SELECT
         a id
      FROM generate_series(1, 1000000) as s(a)
    ),
    dates as(
      select row_number() OVER (ORDER BY a) line, a::date as date
      from generate_series(
            '2020-01-01'::date,
            '2020-12-31'::date,
            '1 day'
          ) s(a)
    )
    select row_number() OVER (ORDER BY id) id,
      d.date,
      200*random() amount
    from nums n, dates d
    where d.line <= 100
  )
  loop
    insert into transactions values (rec.id, rec.date, rec.amount, CURRENT_TIMESTAMP);

    counter := counter + 1;

    if MOD(counter, 10000) = 0 then
      raise notice 'Commiting at : %', counter;
      commit ;
    end if;
  end loop;

  raise notice 'Value: %', counter;

END block $$;
```

