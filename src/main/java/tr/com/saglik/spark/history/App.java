package tr.com.saglik.spark.history;

import java.time.Instant;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        System.out.println( "Hello World!" );
        
/*
 * +-------------+------------------------+
|tmstmpStr    |tmstmp                  |
+-------------+------------------------+
|1593556306700|2020-07-01T01:31:46.700Z|
|1593556304412|2020-07-01T01:31:44.412Z|
|1593556307046|2020-07-01T01:31:47.046Z|
|1593556311217|2020-07-01T01:31:51.217Z|
|1593556307730|2020-07-01T01:31:47.730Z|
|1593556306254|2020-07-01T01:31:46.254Z|
|1593556305955|2020-07-01T01:31:45.955Z|        
 */
        
        System.out.println(Instant.ofEpochMilli(1593556298644L));
    }
}
