import boto3
import yandex_metrika as ym


def upload_to_yc_s3(file_name, df):
    file_name = f"{file_name}.parquet"
    df.to_parquet(file_name, compression=None)
    session = boto3.session.Session(
        aws_access_key_id="ACCESS_KEY",
        aws_secret_access_key="SECRET_KEY",
        region_name="ru-central1",
    )
    s3 = session.client(
        service_name="s3", endpoint_url="https://storage.yandexcloud.net"
    )
    s3.upload_file(file_name, "asemchenko", f"dz1/{file_name}")


def main():
    ######################################################################################
    metrics = "ym:s:visits"
    dimensions = "ym:s:date,ym:s:browserCountry,ym:s:deviceCategory"
    filters = "ym:s:browserCountry=='ru' AND ym:s:deviceCategoryName=='Smartphones'"
    sort = "ym:s:date"
    start_date = "2021-08-03"
    end_date = "2021-08-03"
    df_1 = ym.get_stat_data(metrics, dimensions, filters, sort, start_date, end_date)
    print(df_1.head(20))
    upload_to_yc_s3("ru_smartphones_20210803", df_1)
    ######################################################################################
    metrics = "ym:s:visits,ym:s:bounceRate"
    dimensions = "ym:s:date"
    filters = None
    sort = "ym:s:date"
    start_date = "2021-08-05"
    end_date = "2021-08-05"
    df_2 = ym.get_stat_data(metrics, dimensions, filters, sort, start_date, end_date)
    print(df_2.head(20))
    upload_to_yc_s3("traffic_20210805", df_2)
    ######################################################################################
    metrics = "ym:s:goal30606879reaches"
    dimensions = "ym:s:date"
    filters = None
    sort = "ym:s:date"
    start_date = "2021-07-31"
    end_date = "2021-07-31"
    df_3 = ym.get_stat_data(metrics, dimensions, filters, sort, start_date, end_date)
    print(df_3.head(20))
    upload_to_yc_s3("goal_30606879_20210731", df_3)

    print("Done!")


if __name__ == "__main__":
    main()
