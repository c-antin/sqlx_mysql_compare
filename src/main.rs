use std::{env, time::Instant};

use anyhow::anyhow;
use mysql::{prelude::*, Conn, Opts, Params};

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
        let ts = Instant::now();
        let len = {
            let query = vec!["SELECT 1 FROM DUAL"; n];
            let mut iter = conn.exec_iter::<_, Params>(query.join(" UNION ALL "), ().into())?;
            let mut res = iter.iter().unwrap();
            let mut len = 0;
            while let Some(row) = res.next() {
                let _ = row?;
                len += 1;
            }
            len
        };
        println!("mysql {}s [{}]", ts.elapsed().as_secs_f32(), len);
    }
    Ok(())
}
