from sqlalchemy import Column
from clickhouse_sqlalchemy import types, engines
from clickhouse_sqlalchemy.ext.declarative import declarative_base


Base = declarative_base()


class DmDatamartMonthly(Base):
    """ORM-модель месячной агрегированной витрины абонентов."""
    __tablename__ = "dm_datamart_monthly"
    __table_args__ = (
        engines.Distributed(
            "edwh",
            "DWH",
            "dm_datamart_monthly_src",
            "murmurHash3_64(SUBS_ID)",
        ),
        {"schema": "DWH"},
    )

    dt = Column("DT", types.Date, primary_key=True, comment="Дата начала расчетного периода")
    ctn = Column("CTN", types.String, nullable=False, comment="CTN (номер) абонента")
    subs_id = Column("SUBS_ID", types.UInt64, primary_key=True, comment="Ключ абонента")
    cust_level = Column("CUST_LEVEL", types.Nullable(types.String), comment="Тип клиента")
    status = Column("STATUS", types.Nullable(types.String), comment="Статус абонента")
    region_cell = Column("REGION_CELL", types.String, nullable=False, comment="Регион по последней БС")
    price_plan = Column("PRICE_PLAN", types.String, nullable=False, comment="Тарифный план")
    price_plan_ru = Column("PRICE_PLAN_RU", types.Nullable(types.String), comment="Тарифный план на кириллице")
    periodicity = Column("PERIODICITY", types.String, nullable=False, comment="Периодичность тарифного плана")
    subscription_fee = Column("SUBSCRIPTION_FEE", types.Float32, nullable=False, comment="Стоимость абонентской платы")
    prev_price_plan = Column("PREV_PRICE_PLAN", types.String, nullable=False, comment="Предыдущий тарифный план")
    price_change_date = Column("PRICE_CHANGE_DATE", types.DateTime, nullable=False, comment="Дата последней смены ТП")
    orig_price_plan = Column("ORIG_PRICE_PLAN", types.String, nullable=False, comment="ТП на момент активации")
    act_date = Column("ACT_DATE", types.Nullable(types.DateTime), comment="Дата активации абонента")
    date_inactive = Column("DATE_INACTIVE", types.Date, nullable=False, comment="Дата перехода в неактивный статус")
    balance_end = Column("BALANCE_END", types.Float64, nullable=False, comment="Баланс на конец периода")
    revenue_abonka = Column("REVENUE_ABONKA", types.Float64, nullable=False, comment="Списания за абонплату")
    date_abonka = Column("DATE_ABONKA", types.Nullable(types.DateTime), comment="Дата последнего списания абонплаты")
    usage_abonka_tp = Column("USAGE_ABONKA_TP", types.UInt64, nullable=False, comment="Количество списанных абонплат")
    tranz_flag = Column("TRANZ_FLAG", types.UInt8, nullable=False, comment="Флаг платного события")
    days_without_payment = Column("DAYS_WITHOUT_PAYMENT", types.Int64, nullable=False, comment="Дни без платных транзакций")
    total_recharge = Column("TOTAL_RECHARGE", types.Float64, nullable=False, comment="Сумма пополнений")
    count_recharge = Column("COUNT_RECHARGE", types.UInt64, nullable=False, comment="Количество пополнений")
    date_contract = Column("DATE_CONTRACT", types.Nullable(types.DateTime), comment="Дата персонификации")
    flag_4g = Column("FLAG_4G", types.UInt8, nullable=False, comment="Флаг SIM с поддержкой 4G")
    usage_num_out = Column("USAGE_NUM_OUT", types.UInt64, nullable=False, comment="Количество исходящих звонков")
    usage_out_onnet_voice = Column("USAGE_OUT_ONNET_VOICE", types.Float64, nullable=False)
    usage_out_offnet_voice = Column("USAGE_OUT_OFFNET_VOICE", types.Float64, nullable=False)
    usage_out_city_voice = Column("USAGE_OUT_CITY_VOICE", types.Float64, nullable=False)
    usage_out_int_voice = Column("USAGE_OUT_INT_VOICE", types.Float64, nullable=False)
    usage_out_int_voice_russia = Column("USAGE_OUT_INT_VOICE_RUSSIA", types.Float64, nullable=False)
    usage_in_onnet_voice = Column("USAGE_IN_ONNET_VOICE", types.Float64, nullable=False)
    usage_in_offnet_voice = Column("USAGE_IN_OFFNET_VOICE", types.Float64, nullable=False)
    usage_valueless_internet = Column("USAGE_VALUELESS_INTERNET", types.Float64, nullable=False)
    usage_internet = Column("USAGE_INTERNET", types.Float64, nullable=False)
    usage_internet_2g = Column("USAGE_INTERNET_2G", types.Float64, nullable=False)
    usage_internet_3g = Column("USAGE_INTERNET_3G", types.Float64, nullable=False)
    usage_internet_lte = Column("USAGE_INTERNET_LTE", types.Float64, nullable=False)
    usage_internet_3g_free = Column("USAGE_INTERNET_3G_FREE", types.Float64, nullable=False)
    usage_internet_lte_free = Column("USAGE_INTERNET_LTE_FREE", types.Float64, nullable=False)
    usage_out_offnet_o_voice = Column("USAGE_OUT_OFFNET_O_VOICE", types.Float64, nullable=False)
    usage_out_offnet_megacom_voice = Column("USAGE_OUT_OFFNET_MEGACOM_VOICE", types.Float64, nullable=False)
    usage_in_offnet_o_voice = Column("USAGE_IN_OFFNET_O_VOICE", types.Float64, nullable=False)
    usage_in_offnet_megacom_voice = Column("USAGE_IN_OFFNET_MEGACOM_VOICE", types.Float64, nullable=False)
    count_sms = Column("COUNT_SMS", types.UInt64, nullable=False, comment="Количество SMS")
    revenue_voice = Column("REVENUE_VOICE", types.Float64, nullable=False)
    revenue_voice_to_service = Column("REVENUE_VOICE_TO_SERVICE", types.Float64, nullable=False)
    revenue_out_onnet_voice = Column("REVENUE_OUT_ONNET_VOICE", types.Float64, nullable=False)
    revenue_out_offnet_voice = Column("REVENUE_OUT_OFFNET_VOICE", types.Float64, nullable=False)
    revenue_out_city_voice = Column("REVENUE_OUT_CITY_VOICE", types.Float64, nullable=False)
    revenue_out_int_voice = Column("REVENUE_OUT_INT_VOICE", types.Float64, nullable=False)
    revenue_internet_payg = Column("REVENUE_INTERNET_PAYG", types.Float64, nullable=False)
    usage_internet_night = Column("USAGE_INTERNET_NIGHT", types.Float64, nullable=False)
    usage_num_internet_pak = Column("USAGE_NUM_INTERNET_PAK", types.UInt64, nullable=False)
    usage_num_offnet_pak = Column("USAGE_NUM_OFFNET_PAK", types.UInt64, nullable=False)
    revenue_internet_pak = Column("REVENUE_INTERNET_PAK", types.Float64, nullable=False)
    revenue_offnet_pak = Column("REVENUE_OFFNET_PAK", types.Float64, nullable=False)
    interconnect_mn_in = Column("INTERCONNECT_MN_IN", types.Float64, nullable=False)
    interconnect_mn_out = Column("INTERCONNECT_MN_OUT", types.Float64, nullable=False)
    interconnect_loc_in = Column("INTERCONNECT_LOC_IN", types.Float64, nullable=False)
    interconnect_loc_out = Column("INTERCONNECT_LOC_OUT", types.Float64, nullable=False)
    revenue_total_interconnect = Column("REVENUE_TOTAL_INTERCONNECT", types.Float64, nullable=False)
    gm = Column("GM", types.Float64, nullable=False, comment="Gross margin")
    ivr_lang = Column("IVR_LANG", types.String, nullable=False, comment="Значение услуги 'Свой язык'")
    tac = Column("TAC", types.Nullable(types.String), comment="Первые 8 символов IMEI")
    cell_id = Column("CELL_ID", types.String, nullable=False)
    dev_name = Column("DEV_NAME", types.String, nullable=False)
    dev_type = Column("DEV_TYPE", types.String, nullable=False)
    flag_device_4g = Column("FLAG_DEVICE_4G", types.String, nullable=False, comment="Флаг поддержки 4G устройством")
    os_name = Column("OS_NAME", types.String, nullable=False)
    date_lad = Column("DATE_LAD", types.Nullable(types.Date), comment="Дата последнего активного действия")
    revenue_total = Column("REVENUE_TOTAL", types.Float64, nullable=False)
    other_charges = Column("OTHER_CHARGES", types.Float64, nullable=False)
    active_ind = Column("ACTIVE_IND", types.Int16, nullable=False, comment="Индикатор активности абонента")
    usage_out_free_offnet_voice = Column("USAGE_OUT_FREE_OFFNET_VOICE", types.Float64, nullable=False)
    revenue_daily_abonka = Column("REVENUE_DAILY_ABONKA", types.Float64, nullable=False)
    usage_daily_abonka = Column("USAGE_DAILY_ABONKA", types.UInt64, nullable=False)
    region = Column("REGION", types.String, nullable=False)
    revenue_rouming = Column("REVENUE_ROUMING", types.Float64, nullable=False)
    imei = Column("IMEI", types.Nullable(types.String), comment="IMEI устройства")
    usage_num_inc = Column("USAGE_NUM_INC", types.UInt64, nullable=False, comment="Количество входящих звонков")
    revenue_offnet_o_voice = Column("REVENUE_OFFNET_O_VOICE", types.Float64, nullable=False)
    revenue_offnet_megacom_voice = Column("REVENUE_OFFNET_MEGACOM_VOICE", types.Float64, nullable=False)
    cell_max = Column("CELL_MAX", types.String, nullable=False)
    roly_voice_charge = Column("ROLY_VOICE_CHARGE", types.Float64, nullable=False)
    roly_data_charge = Column("ROLY_DATA_CHARGE", types.Float64, nullable=False)
    roly_global = Column("ROLY_GLOBAL", types.Float64, nullable=False)
    flag_abonka = Column("FLAG_ABONKA", types.UInt8, nullable=False)
    total_mou = Column("TOTAL_MOU", types.Float64, nullable=False)
    first_sim = Column("FIRST_SIM", types.UInt8, nullable=False)
    my_beeline_user = Column("MY_BEELINE_USER", types.UInt8, nullable=False)
    balance_user = Column("BALANCE_USER", types.UInt8, nullable=False)
    multiplay = Column("MULTIPLAY", types.UInt8, nullable=False)
    lifetime_total = Column("LIFETIME_TOTAL", types.Int32, nullable=False)
    m2m_flag = Column("M2M_FLAG", types.UInt8, nullable=False)
    gender = Column("GENDER", types.String, nullable=False)
    age = Column("AGE", types.String, nullable=False)


class HfctSubsShort(Base):
    """Минимальная ORM-модель витрины подписок для отбора eligible-абонентов."""
    __tablename__ = "hfct_subs_short"
    __table_args__ = (
        engines.Distributed(
            "edwh",
            "DWH",
            "hfct_subs_src",
            "rand()",
        ),
        {"schema": "DWH"},
    )

    eff_dt = Column("eff_dt", types.Date, primary_key=True, comment="Дата среза")
    subscription_id = Column("subscription_id", types.UInt64, primary_key=True, comment="Идентификатор подписки")
    subs_termination_time = Column("subs_termination_time", types.Nullable(types.DateTime))
    subs_msisdn = Column("subs_msisdn", types.String, nullable=False, comment="MSISDN абонента")
    subs_category = Column("subs_category", types.Nullable(types.String))
    contract_status = Column("contract_status", types.Nullable(types.UInt8))
