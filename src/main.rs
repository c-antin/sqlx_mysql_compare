use std::{env, time::Instant};

use anyhow::anyhow;
use mysql::{prelude::*, Conn, Opts, Params};
use sqlx::{Connection, Executor, MySqlConnection, QueryBuilder};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let url = env::var("MYSQL_URL")?;
    let mut args = env::args();
    let _ = args.next();
    let arg1 = args.next();
    if arg1.is_none() {
        Err(anyhow!("arg1 missing!"))?;
    }
    let n = arg1.unwrap().parse::<usize>()?;
    //setup
    {
        let mut conn = MySqlConnection::connect(&url).await?;
        conn.execute("CREATE DATABASE IF NOT EXISTS sqlx_mysql_compare")
            .await?;
        conn.execute("USE sqlx_mysql_compare").await?;
        conn.execute(
            "CREATE TABLE IF NOT EXISTS ids (id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY)",
        )
        .await?;
        conn.execute("TRUNCATE TABLE ids").await?;
        let mut builder = QueryBuilder::new("INSERT INTO ids (id) ");
        builder.push_values(vec![(); n], |mut b, _| {
            b.push("DEFAULT");
        });
        let query = builder.build();
        query.execute(&mut conn).await?;
    }
    //mysql
    {
        let opts = Opts::from_url(&url)?;
        let mut conn = Conn::new(opts)?;
        let _ = conn.query_drop("USE sqlx_mysql_compare");
        let query = format!("SELECT * FROM ids LIMIT {}", n);
        let ts = Instant::now();
        let len = {
            let mut res = conn.exec_iter::<_, Params>(&query, ().into())?;
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
        conn.execute("USE sqlx_mysql_compare").await?;
        let mut builder = QueryBuilder::new(&format!("SELECT * FROM ids LIMIT {}", n));
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
