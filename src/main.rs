use std::{env, time::Instant};

use anyhow::anyhow;
use mysql::{prelude::*, Conn, Opts, Params};
use sqlx::{Connection, MySqlConnection, QueryBuilder};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let url = env::var("MYSQL_URL")?;
    let mut args = env::args();
    let _ = args.next();
    let arg1 = args.next();
    if arg1.is_none() {
        Err(anyhow!("arg1 missing!"))?;
    }
    let n = arg1.unwrap().parse()?;
    //mysql
    {
        let opts = Opts::from_url(&url)?;
        let mut conn = Conn::new(opts)?;
        let queries = vec!["SELECT 1 FROM DUAL"; n];
        let ts = Instant::now();
        let len = {
            let mut res = conn.exec_iter::<_, Params>(queries.join(" UNION ALL "), ().into())?;
            let mut iter = res.iter().unwrap();
            let mut len = 0;
            while let Some(row) = iter.next() {
                let _ = row?;
                len += 1;
            }
            len
        };
        println!("mysql {}s [{}]", ts.elapsed().as_secs_f32(), len);
    }
    //sqlx
    {
        let mut conn = MySqlConnection::connect(&url).await?;
        let mut queries = vec!["SELECT 1 FROM DUAL"; n];
        let mut builder = QueryBuilder::new(queries.pop().unwrap());
        for query in queries {
            builder.push(&format!(" UNION ALL {}", query));
        }
        let query = builder.build();
        let ts = Instant::now();
        let len = {
            let res = query.fetch_all(&mut conn).await?;
            let mut iter = res.iter();
            let mut len = 0;
            while let Some(row) = iter.next() {
                let _ = row;
                len += 1;
            }
            len
        };
        println!("sqlx {}s [{}]", ts.elapsed().as_secs_f32(), len);
    }
    Ok(())
}
