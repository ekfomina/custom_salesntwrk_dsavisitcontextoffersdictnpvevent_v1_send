from datetime import datetime

import pytest
from pyspark.sql.types import IntegerType
from pyspark.sql.types import StringType
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType
from pyspark.sql.types import TimestampType

# from scripts.work_mode import ArchiveMode
# from scripts.work_mode import IncrementMode
# from scripts.work_mode import PointDateMode
from scripts.work_mode import RowWithMetadata
from scripts.work_mode import SyntheticMode
from scripts.work_mode import select_mode


@pytest.fixture(scope='function')
def synthetic_table(spark_session) -> (str, str):
    """
    Создает синтетическую таблицу с данными которе нужно в кафку перегрузить
    В указанной схеме с указанным название

    После выхода из указанной области действия, таблица удалится
    """

    schema = 'default'
    name = 'synthetic'

    data = [('{  "UCPId": "50235802834",  "Offers": [    {      "DefaultOffer": [{        "AttrOffer": [          {            "Name": "Source",            "Value": "CaaS"          },          {            "Name": "Name",            "Value": "Лучшее предложение кредита"          },          {            "Name": "Order",            "Value": "1"          }        ],        "ProductOfferList": [          {            "AttrProdOffer": [              {                "Name": "LineId",                "Value": "1"              },              {                "Name": "UnqProductCode",                "Value": "ZASHCHITA_OT_TRAVM_24_09_2020"              },              {                "Name": "ProductId",                "Value": "1-1IMOBGG6"              },              {                "Name": "RateMax",                "Value": "0.07"              },              {                "Name": "RateMin",                "Value": "0.01"              }                                        ]          }        ]      }],      "Step": "KDV",      "UserRole": "MP",      "ScriptOffers": [        {          "OfferList": [            {              "AttrOffer": [                {                  "Name": "Source",                  "Value": "CaaS"                },                {                  "Name": "Name",                  "Value": "Лучшее предложение кредита"                },                {                  "Name": "Order",                  "Value": "1"                }              ],              "ProductOfferList": [                {                  "AttrProdOffer": [                    {                      "Name": "LineId",                      "Value": "1"                    },                    {                      "Name": "UnqProductCode",                      "Value": "ZASHCHITA_OT_TRAVM_24_09_2020"                    },                    {                      "Name": "ProductId",                      "Value": "1-1IMOBGG6"                    }                  ]                }              ]            }          ],          "ScriptName": "Дебетовая карта.Новая"        }      ]    }  ]}',),
            ('{  "UCPId": "50235802834",  "Offers": [    {      "DefaultOffer": [{        "AttrOffer": [          {            "Name": "Source",            "Value": "CaaS"          },          {            "Name": "Name",            "Value": "Лучшее предложение кредита"          },          {            "Name": "Order",            "Value": "1"          }        ],        "ProductOfferList": [          {            "AttrProdOffer": [              {                "Name": "LineId",                "Value": "1"              },              {                "Name": "UnqProductCode",                "Value": "ZASHCHITA_OT_TRAVM_24_09_2020"              },              {                "Name": "ProductId",                "Value": "1-1IMOBGG6"              },              {                "Name": "RateMax",                "Value": "0.07"              },              {                "Name":"Column2",                "Value":"Ежемесячный взнос"              }                                        ]          }        ]      },      {        "AttrOffer": [          {            "Name": "Source",            "Value": "CaaS"          },          {            "Name": "Name",            "Value": "Лучшее предложение СберПрайма"          },          {            "Name": "Order",            "Value": "2"          }        ],        "ProductOfferList": [          {            "AttrProdOffer": [              {                "Name": "LineId",                "Value": "1"              },              {                "Name": "UnqProductCode",                "Value": "SberPrime"              },              {                "Name": "ProductId",                "Value": "1-1IMOBGG3"              },              {                 "Name":"Cell1",                  "Value":"147000"              }            ]          }        ]      }],      "Step": "KDV",      "UserRole": "MP",      "ScriptOffers": [        {          "OfferList": [            {              "AttrOffer": [                {                  "Name": "Source",                  "Value": "CaaS"                },                {                  "Name": "Name",                  "Value": "Лучшее предложение кредита"                },                {                  "Name": "Order",                  "Value": "1"                }              ],              "ProductOfferList": [                {                  "AttrProdOffer": [                    {                      "Name": "LineId",                      "Value": "1"                    },                    {                      "Name": "UnqProductCode",                      "Value": "ZASHCHITA_OT_TRAVM_24_09_2020"                    },                    {                      "Name": "ProductId",                      "Value": "1-1IMOBGG6"                    }                  ]                }              ]            }          ],          "ScriptName": "Дебетовая карта.Новая"        }      ]    }  ]}',),
            ('{  "UCPId": "50235802834",  "Offers": [    {      "DefaultOffer": [{        "AttrOffer": [          {            "Name": "Source",            "Value": "CaaS"          },          {            "Name": "Name",            "Value": "Лучшее предложение кредита"          },          {            "Name": "Order",            "Value": "1"          }        ],        "ProductOfferList": [          {            "AttrProdOffer": [              {                "Name": "LineId",                "Value": "1"              },              {                "Name": "UnqProductCode",                "Value": "ZASHCHITA_OT_TRAVM_24_09_2020"              },              {                "Name": "ProductId",                "Value": "1-1IMOBGG6"              },              {                "Name": "RateMax",                "Value": "0.07"              },              {                "Name":"Column2",                "Value":"Ежемесячный взнос"              }                                        ]          }        ]      },      {        "AttrOffer": [          {            "Name": "Source",            "Value": "CaaS"          },          {            "Name": "Name",            "Value": "Лучшее предложение СберПрайма"          },          {            "Name": "Order",            "Value": "2"          }        ],        "ProductOfferList": [          {            "AttrProdOffer": [              {                "Name": "LineId",                "Value": "1"              },              {                "Name": "UnqProductCode",                "Value": "SberPrime"              },              {                "Name": "ProductId",                "Value": "1-1IMOBGG3"              },              {                 "Name":"Cell1",                  "Value":"147000"              }            ]          }        ]      }],      "Step": "KDV",      "UserRole": "MP",      "ScriptOffers": [        {          "OfferList": [            {              "AttrOffer": [                {                  "Name": "Source",                  "Value": "CaaS"                },                {                  "Name": "Name",                  "Value": "Лучшее предложение кредита"                },                {                  "Name": "Order",                  "Value": "1"                }              ],              "ProductOfferList": [                {                  "AttrProdOffer": [                    {                      "Name": "LineId",                      "Value": "1"                    },                    {                      "Name": "UnqProductCode",                      "Value": "ZASHCHITA_OT_TRAVM_24_09_2020"                    },                    {                      "Name": "ProductId",                      "Value": "1-1IMOBGG6"                    }                  ]                }              ]            }          ],          "ScriptName": "Дебетовая карта.Новая"        },        {          "OfferList": [            {              "AttrOffer": [                {                  "Name": "Source",                  "Value": "CaaS"                },                {                  "Name": "Name",                  "Value": "Лучшее предложение ДСЖ"                },                {                  "Name": "Order",                  "Value": "1"                }              ],              "ProductOfferList": [                {                  "AttrProdOffer": [                    {                      "Name": "LineId",                      "Value": "1"                    },                    {                      "Name": "UnqProductCode",                      "Value": "ДСЖ"                    },                    {                      "Name": "ProductId",                      "Value": "1-1IM2GG6"                    }                  ]                }              ]            }          ],          "ScriptName": "Кредитная карта"        }      ]    }  ]}',),
            ('{  "UCPId": "50235802834",  "Offers": [    {      "DefaultOffer": [{        "AttrOffer": [          {            "Name": "Source",            "Value": "CaaS"          },          {            "Name": "Name",            "Value": "Лучшее предложение кредита"          },          {            "Name": "Order",            "Value": "1"          }        ],        "ProductOfferList": [          {            "AttrProdOffer": [              {                "Name": "LineId",                "Value": "1"              },              {                "Name": "UnqProductCode",                "Value": "ZASHCHITA_OT_TRAVM_24_09_2020"              },              {                "Name": "ProductId",                "Value": "1-1IMOBGG6"              },              {                "Name": "TABLE_NAME",                "Value": "Предодобренный лимит по кредиту"              },              {                "Name": "CURRENCY",                "Value": "RUB"              },              {                "Name": "V_MIN",                "Value": "16.9"              },              {                "Name": "V_MAX",                "Value": "16.9"              },              {                "Name": "LIMIT_MIN",                "Value": "50000"              },              {                "Name": "LIMIT_MAX",                "Value": "1500000"              },              {                "Name": "PERIOD_MIN",                "Value": "2"              },              {                "Name": "PERIOD_MAX",                "Value": "430"              },              {                "Name":"V1",                "Value":"6 мес."              },              {                "Name":"V2",                "Value":"12 мес."              },              {                "Name":"V3",                "Value":"18 мес."              },              {                "Name":"V4",                "Value":"24 мес."              },              {                "Name":"V5",                "Value":"36 мес."              },              {                "Name":"V6",                "Value":"48 мес."              },              {                "Name":"V7",                "Value":"60 мес."              },              {                "Name":"V8",                "Value":"17.9"              },              {                "Name":"V9",                "Value":"Сумма наличными"              },              {                "Name":"V15",                "Value":"100000"              },              {                "Name":"V16",                "Value":"200000"              },              {                "Name":"V17",                "Value":"300000"              },              {                "Name":"V18",                "Value":"500000"              },              {                "Name":"V19",                "Value":"750000"              },              {                "Name":"V20",                "Value":"1000000"              },              {                "Name":"V21",                "Value":"1500000"              },              {                "Name":"V22",                "Value":"50000"              },              {                "Name":"V23",                "Value":"150000"              },              {                "Name":"V24",                "Value":"250000"              },              {                "Name":"V25",                "Value":"450000"              },              {                "Name":"V26",                "Value":"700000"              },              {                "Name":"V27",                "Value":"950000"              },              {                "Name":"V28",                "Value":"1450000"              }            ]          }        ]      },      {        "AttrOffer": [          {            "Name": "Source",            "Value": "CaaS"          },          {            "Name": "Name",            "Value": "Лучшее предложение СберПрайма"          },          {            "Name": "Order",            "Value": "2"          }        ],        "ProductOfferList": [          {            "AttrProdOffer": [              {                "Name": "LineId",                "Value": "1"              },              {                "Name": "UnqProductCode",                "Value": "SberPrime"              },              {                "Name": "ProductId",                "Value": "1-1IMOBGG3"              },              {                 "Name":"Cell1",                  "Value":"147000"              }            ]          }        ]      }],      "Step": "KDV",      "UserRole": "MP",      "ScriptOffers": [        {          "OfferList": [            {              "AttrOffer": [                {                  "Name": "Source",                  "Value": "CaaS"                },                {                  "Name": "Name",                  "Value": "Лучшее предложение кредита"                },                {                  "Name": "Order",                  "Value": "1"                }              ],              "ProductOfferList": [                {                  "AttrProdOffer": [                    {                      "Name": "LineId",                      "Value": "1"                    },                    {                      "Name": "UnqProductCode",                      "Value": "ZASHCHITA_OT_TRAVM_24_09_2020"                    },                    {                      "Name": "ProductId",                      "Value": "1-1IMOBGG6"                    }                  ]                }              ]            }          ],          "ScriptName": "Дебетовая карта.Новая"        },        {          "OfferList": [            {              "AttrOffer": [                {                  "Name": "Source",                  "Value": "CaaS"                },                {                  "Name": "Name",                  "Value": "Лучшее предложение ДСЖ"                },                {                  "Name": "Order",                  "Value": "1"                }              ],              "ProductOfferList": [                {                  "AttrProdOffer": [                    {                      "Name": "LineId",                      "Value": "1"                    },                    {                      "Name": "UnqProductCode",                      "Value": "ДСЖ"                    },                    {                      "Name": "ProductId",                      "Value": "1-1IM2GG6"                    }                  ]                }              ]            }          ],          "ScriptName": "Кредитная карта"        }      ]    }  ]}',),
            ('{  "UCPId": "252023580283424",  "Offers": [    {      "DefaultOffer": [{        "AttrOffer": [          {            "Name": "Source",            "Value": "CaaS"          },          {            "Name": "Name",            "Value": "Лучшее предложение кредита"          },          {            "Name": "Order",            "Value": "1"          }        ],        "ProductOfferList": [          {            "AttrProdOffer": [              {                "Name": "LineId",                "Value": "1"              },              {                "Name": "UnqProductCode",                "Value": "ZASHCHITA_OT_TRAVM_24_09_2020"              },              {                "Name": "ProductId",                "Value": "1-1IMOBGG6"              },              {                "Name": "TABLE_NAME",                "Value": "Предодобренный лимит по кредиту"              },              {                "Name": "CURRENCY",                "Value": "RUB"              },              {                "Name": "V_MIN",                "Value": "16.9"              },              {                "Name": "V_MAX",                "Value": "16.9"              },              {                "Name": "LIMIT_MIN",                "Value": "50000"              },              {                "Name": "LIMIT_MAX",                "Value": "1500000"              },              {                "Name": "PERIOD_MIN",                "Value": "2"              },              {                "Name": "PERIOD_MAX",                "Value": "430"              },              {                "Name":"V1",                "Value":"6 мес."              },              {                "Name":"V2",                "Value":"12 мес."              },              {                "Name":"V3",                "Value":"18 мес."              },              {                "Name":"V4",                "Value":"24 мес."              },              {                "Name":"V5",                "Value":"36 мес."              },              {                "Name":"V6",                "Value":"48 мес."              },              {                "Name":"V7",                "Value":"60 мес."              },              {                "Name":"V8",                "Value":"17.9"              },              {                "Name":"V9",                "Value":"Сумма наличными"              },              {                "Name":"V15",                "Value":"100000"              },              {                "Name":"V16",                "Value":"200000"              },              {                "Name":"V17",                "Value":"300000"              },              {                "Name":"V18",                "Value":"500000"              },              {                "Name":"V19",                "Value":"750000"              },              {                "Name":"V20",                "Value":"1000000"              },              {                "Name":"V21",                "Value":"1500000"              },              {                "Name":"V22",                "Value":"50000"              },              {                "Name":"V23",                "Value":"150000"              },              {                "Name":"V24",                "Value":"250000"              },              {                "Name":"V25",                "Value":"450000"              },              {                "Name":"V26",                "Value":"700000"              },              {                "Name":"V27",                "Value":"950000"              },              {                "Name":"V28",                "Value":"1450000"              }            ]          }        ]      },      {        "AttrOffer": [          {            "Name": "Source",            "Value": "CaaS"          },          {            "Name": "Name",            "Value": "Лучшее предложение СберПрайма"          },          {            "Name": "Order",            "Value": "2"          }        ],        "ProductOfferList": [          {            "AttrProdOffer": [              {                "Name": "LineId",                "Value": "1"              },              {                "Name": "UnqProductCode",                "Value": "SberPrime"              },              {                "Name": "ProductId",                "Value": "1-1IMOBGG3"              },              {                 "Name":"V1",                  "Value":"T01"              },              {                 "Name":"V2",                  "Value":"T02"              },              {                 "Name":"V3",                  "Value":"T03"              },              {                 "Name":"V4",                  "Value":"T04"              },              {                 "Name":"V5",                  "Value":"T05"              },              {                 "Name":"V6",                  "Value":"T06"              },              {                 "Name":"V7",                  "Value":"T07"              },              {                 "Name":"V8",                  "Value":"1"              },              {                 "Name":"V9",                  "Value":"2"              },              {                 "Name":"V10",                  "Value":"3"              },              {                 "Name":"V11",                  "Value":"4"              },              {                 "Name":"V12",                  "Value":"5"              },              {                 "Name":"V13",                  "Value":"6"              },              {                 "Name":"V14",                  "Value":"7"              },              {                 "Name":"V15",                  "Value":"14124 / 42141"              },              {                 "Name":"V16",                  "Value":"242 / 21"              },              {                 "Name":"V17",                  "Value":"76 / 3"              },              {                 "Name":"V18",                  "Value":"2 / 5"              },              {                 "Name":"V19",                  "Value":"2 / 2"              },              {                 "Name":"V20",                  "Value":"0 / 0"              },              {                 "Name":"V21",                  "Value":"14124 / 42141"              }            ]          }        ]      }],      "Step": "KDV",      "UserRole": "MP",      "ScriptOffers": [        {          "OfferList": [            {              "AttrOffer": [                {                  "Name": "Source",                  "Value": "CaaS"                },                {                  "Name": "Name",                  "Value": "Лучшее предложение кредита"                },                {                  "Name": "Order",                  "Value": "1"                }              ],              "ProductOfferList": [                {                  "AttrProdOffer": [                    {                      "Name": "LineId",                      "Value": "1"                    },                    {                      "Name": "UnqProductCode",                      "Value": "ZASHCHITA_OT_TRAVM_24_09_2020"                    },                    {                      "Name": "ProductId",                      "Value": "1-1IMOBGG6"                    }                  ]                }              ]            }          ],          "ScriptName": "Дебетовая карта.Новая"        },        {          "OfferList": [            {              "AttrOffer": [                {                  "Name": "Source",                  "Value": "CaaS"                },                {                  "Name": "Name",                  "Value": "Лучшее предложение ДСЖ"                },                {                  "Name": "Order",                  "Value": "1"                }              ],              "ProductOfferList": [                {                  "AttrProdOffer": [                    {                      "Name": "LineId",                      "Value": "1"                    },                    {                      "Name": "UnqProductCode",                      "Value": "ДСЖ"                    },                    {                      "Name": "ProductId",                      "Value": "1-1IM2GG6"                    }                  ]                }              ]            }          ],          "ScriptName": "Кредитная карта"        }      ]    }  ]}',),
            ('{  "UCPId": "50235802834",  "Offers": [    {      "DefaultOffer": [{        "AttrOffer": [          {            "Name": "Source",            "Value": "CaaS"          },          {            "Name": "Name",            "Value": "Лучшее предложение кредита"          },          {            "Name": "Order",            "Value": "1"          }        ],        "ProductOfferList": [          {            "AttrProdOffer": [              {                "Name": "LineId",                "Value": "1"              },              {                "Name": "UnqProductCode",                "Value": "KREDITNAYA_KARTA"              },              {                "Name": "ProductId",                "Value": "1-2IMOOGG6"              },              {                "Name": "V_MIN",                "Value": "23.90"              },              {                "Name":"V_MAX",                "Value":"23.90"              },                            {                "Name":"LIMIT_MIN",                "Value":"23.90"              },                            {                "Name":"LIMIT_MAX",                "Value":"23.90"              },                            {                "Name":"BANK",                "Value":"23.90"              },                            {                "Name":"VSP",                "Value":"00217"              },              {                "Name":"V6",                "Value": "17.90"              },              {                "Name":"V7",                "Value": "17.90"              },              {                "Name":"V8",                "Value": "50 000"              },              {                "Name":"V9",                "Value": "165 000"              },              {                "Name":"V21",                "Value": "70450"              },              {                "Name":"V35",                "Value": "165.000"              },              {                "Name":"V56",                "Value": "3.1.34"              },              {                "Name":"V58",                "Value": "42"              },              {                "Name":"V64",                "Value": "42"              },              {                "Name":"V65",                "Value": "42"              }            ]          }        ]      },      {        "AttrOffer": [          {            "Name": "Source",            "Value": "CaaS"          },          {            "Name": "Name",            "Value": "Лучшее предложение СберПрайма"          },          {            "Name": "Order",            "Value": "2"          }        ],        "ProductOfferList": [          {            "AttrProdOffer": [              {                "Name": "LineId",                "Value": "1"              },              {                "Name": "UnqProductCode",                "Value": "SberPrime"              },              {                "Name": "ProductId",                "Value": "1-1IMOBGG3"              },              {                 "Name":"Cell1",                  "Value":"147000"              }            ]          }        ]      }],      "Step": "KDV",      "UserRole": "MP",      "ScriptOffers": [        {          "OfferList": [            {              "AttrOffer": [                {                  "Name": "Source",                  "Value": "CaaS"                },                {                  "Name": "Name",                  "Value": "Лучшее предложение кредита"                },                {                  "Name": "Order",                  "Value": "1"                }              ],              "ProductOfferList": [                {                  "AttrProdOffer": [                    {                      "Name": "LineId",                      "Value": "1"                    },                    {                      "Name": "UnqProductCode",                      "Value": "ZASHCHITA_OT_TRAVM_24_09_2020"                    },                    {                      "Name": "ProductId",                      "Value": "1-1IMOBGG6"                    }                  ]                }              ]            }          ],          "ScriptName": "Дебетовая карта.Новая"        },        {          "OfferList": [            {              "AttrOffer": [                {                  "Name": "Source",                  "Value": "CaaS"                },                {                  "Name": "Name",                  "Value": "Лучшее предложение ДСЖ"                },                {                  "Name": "Order",                  "Value": "1"                }              ],              "ProductOfferList": [                {                  "AttrProdOffer": [                    {                      "Name": "LineId",                      "Value": "1"                    },                    {                      "Name": "UnqProductCode",                      "Value": "ДСЖ"                    },                    {                      "Name": "ProductId",                      "Value": "1-1IM2GG6"                    }                  ]                }              ]            }          ],          "ScriptName": "Кредитная карта"        }      ]    }  ]}',),
            ('{  "UCPId": "252023580283424",  "Offers": [    {      "DefaultOffer": [{        "AttrOffer": [          {            "Name": "Source",            "Value": "CaaS"          },          {            "Name": "Name",            "Value": "Лучшее предложение кредита"          },          {            "Name": "Order",            "Value": "1"          }        ],        "ProductOfferList": [          {            "AttrProdOffer": [              {                "Name": "LineId",                "Value": "1"              },              {                "Name": "UnqProductCode",                "Value": "ZASHCHITA_OT_TRAVM_24_09_2020"              },              {                "Name": "ProductId",                "Value": "1-1IMOBGG6"              },              {                "Name": "TABLE_NAME",                "Value": "Предодобренный лимит по кредиту"              },              {                "Name": "CURRENCY",                "Value": "RUB"              },              {                "Name": "V_MIN",                "Value": "16.9"              },              {                "Name": "V_MAX",                "Value": "16.9"              },              {                "Name": "LIMIT_MIN",                "Value": "50000"              },              {                "Name": "LIMIT_MAX",                "Value": "1500000"              },              {                "Name": "PERIOD_MIN",                "Value": "2"              },              {                "Name": "PERIOD_MAX",                "Value": "430"              },              {                "Name":"V1",                "Value":"6 мес."              },              {                "Name":"V2",                "Value":"12 мес."              },              {                "Name":"V3",                "Value":"18 мес."              },              {                "Name":"V4",                "Value":"24 мес."              },              {                "Name":"V5",                "Value":"36 мес."              },              {                "Name":"V6",                "Value":"48 мес."              },              {                "Name":"V7",                "Value":"60 мес."              },              {                "Name":"V8",                "Value":"17.9"              },              {                "Name":"V9",                "Value":"Сумма наличными"              },              {                "Name":"V15",                "Value":"100000"              },              {                "Name":"V16",                "Value":"200000"              },              {                "Name":"V17",                "Value":"300000"              },              {                "Name":"V18",                "Value":"500000"              },              {                "Name":"V19",                "Value":"750000"              },              {                "Name":"V20",                "Value":"1000000"              },              {                "Name":"V21",                "Value":"1500000"              },              {                "Name":"V22",                "Value":"50000"              },              {                "Name":"V23",                "Value":"150000"              },              {                "Name":"V24",                "Value":"250000"              },              {                "Name":"V25",                "Value":"450000"              },              {                "Name":"V26",                "Value":"700000"              },              {                "Name":"V27",                "Value":"950000"              },              {                "Name":"V28",                "Value":"1450000"              }            ]          }        ]      },      {        "AttrOffer": [          {            "Name": "Source",            "Value": "CaaS"          },          {            "Name": "Name",            "Value": "Лучшее предложение СберПрайма"          },          {            "Name": "Order",            "Value": "2"          }        ],        "ProductOfferList": [          {            "AttrProdOffer": [              {                "Name": "LineId",                "Value": "1"              },              {                "Name": "UnqProductCode",                "Value": "SberPrime"              },              {                "Name": "ProductId",                "Value": "1-1IMOBGG3"              },              {                 "Name":"V1",                  "Value":"T01"              },              {                 "Name":"V2",                  "Value":"T02"              },              {                 "Name":"V3",                  "Value":"T03"              },              {                 "Name":"V4",                  "Value":"T04"              },              {                 "Name":"V5",                  "Value":"T05"              },              {                 "Name":"V6",                  "Value":"T06"              },              {                 "Name":"V7",                  "Value":"T07"              },              {                 "Name":"V8",                  "Value":"1"              },              {                 "Name":"V9",                  "Value":"2"              },              {                 "Name":"V10",                  "Value":"3"              },              {                 "Name":"V11",                  "Value":"4"              },              {                 "Name":"V12",                  "Value":"5"              },              {                 "Name":"V13",                  "Value":"6"              },              {                 "Name":"V14",                  "Value":"7"              },              {                 "Name":"V15",                  "Value":"14124 / 42141"              },              {                 "Name":"V16",                  "Value":"242 / 21"              },              {                 "Name":"V17",                  "Value":"76 / 3"              },              {                 "Name":"V18",                  "Value":"2 / 5"              },              {                 "Name":"V19",                  "Value":"2 / 2"              },              {                 "Name":"V20",                  "Value":"0 / 0"              },              {                 "Name":"V21",                  "Value":"14124 / 42141"              }            ]          }        ]      }],      "Step": "KDV",      "UserRole": "MP",      "ScriptOffers": [        {          "OfferList": [            {              "AttrOffer": [                {                  "Name": "Source",                  "Value": "CaaS"                },                {                  "Name": "Name",                  "Value": "Лучшее предложение кредита"                },                {                  "Name": "Order",                  "Value": "1"                }              ],              "ProductOfferList": [                {                  "AttrProdOffer": [                    {                      "Name": "LineId",                      "Value": "1"                    },                    {                      "Name": "UnqProductCode",                      "Value": "ZASHCHITA_OT_TRAVM_24_09_2020"                    },                    {                      "Name": "ProductId",                      "Value": "1-1IMOBGG6"                    }                  ]                }              ]            }          ],          "ScriptName": "Дебетовая карта.Новая"        },        {          "OfferList": [            {              "AttrOffer": [                {                  "Name": "Source",                  "Value": "CaaS"                },                {                  "Name": "Name",                  "Value": "Лучшее предложение ДСЖ"                },                {                  "Name": "Order",                  "Value": "1"                }              ],              "ProductOfferList": [                {                  "AttrProdOffer": [                    {                      "Name": "LineId",                      "Value": "1"                    },                    {                      "Name": "UnqProductCode",                      "Value": "ДСЖ"                    },                    {                      "Name": "ProductId",                      "Value": "1-1IM2GG6"                    }                  ]                }              ]            }          ],          "ScriptName": "Кредитная карта"        },        {          "OfferList": [            {              "AttrOffer": [                {                  "Name": "Source",                  "Value": "CaaS"                },                {                  "Name": "Name",                  "Value": "Лучшее предложение ДСЖ"                },                {                  "Name": "Order",                  "Value": "1"                }              ],              "ProductOfferList": [                {                  "AttrProdOffer": [                    {                      "Name": "LineId",                      "Value": "1"                    },                    {                      "Name": "UnqProductCode",                      "Value": "ДСЖ"                    },                    {                      "Name": "ProductId",                      "Value": "1-1IM2GG6"                    }                  ]                }              ]            },            {              "AttrOffer": [                {                  "Name": "Source",                  "Value": "CaaS"                },                {                  "Name": "Name",                  "Value": "Лучшее предложение Ипотека"                },                {                  "Name": "Order",                  "Value": "2"                }              ],              "ProductOfferList": [                {                  "AttrProdOffer": [                    {                      "Name": "LineId",                      "Value": "1"                    },                    {                      "Name": "UnqProductCode",                      "Value": "Ипотека"                    },                    {                      "Name": "ProductId",                      "Value": "1-1IM3GG6"                    }                  ]                }              ]            },            {              "AttrOffer": [                {                  "Name": "Source",                  "Value": "CaaS"                },                {                  "Name": "Name",                  "Value": "Лучшее предложение Самокат"                },                {                  "Name": "Order",                  "Value": "3"                }              ],              "ProductOfferList": [                {                  "AttrProdOffer": [                    {                      "Name": "LineId",                      "Value": "1"                    },                    {                      "Name": "UnqProductCode",                      "Value": "Ипотека"                    },                    {                      "Name": "ProductId",                      "Value": "1-1IM3GG6"                    }                  ]                }              ]            }          ],          "ScriptName": "Вклад"        }      ]    }  ]}',),
            ('{  "UCPId": "50235802834",  "Offers": [    {      "DefaultOffer": [{        "AttrOffer": [          {            "Name": "Source",            "Value": "CaaS"          },          {            "Name": "Name",            "Value": "Лучшее предложение кредита"          },          {            "Name": "Order",            "Value": "1"          }        ],        "ProductOfferList": [          {            "AttrProdOffer": [              {                "Name": "LineId",                "Value": "1"              },              {                "Name": "UnqProductCode",                "Value": "ZASHCHITA_OT_TRAVM_24_09_2020"              },              {                "Name": "ProductId",                "Value": "1-1IMOBGG6"              },              {                "Name": "RateMax",                "Value": "0.07"              },              {                "Name": "RateMin",                "Value": "0.01"              }                                          ]          }        ]      }],      "Step": "KDV",      "UserRole": "MP",      "ScriptOffers": [        {          "OfferList": [            {              "AttrOffer": [                {                  "Name": "Source",                  "Value": "CaaS"                },                {                  "Name": "Name",                  "Value": "Лучшее предложение кредита"                },                {                  "Name": "Order",                  "Value": "1"                }              ],              "ProductOfferList": [                {                  "AttrProdOffer": [                    {                      "Name": "LineId",                      "Value": "1"                    },                    {                      "Name": "UnqProductCode",                      "Value": "ZASHCHITA_OT_TRAVM_24_09_2020"                    },                    {                      "Name": "ProductId",                      "Value": "1-1IMOBGG6"                    }                  ]                },                {                  "AttrProdOffer": [                    {                      "Name": "LineId",                      "Value": "2"                    },                    {                      "Name": "UnqProductCode",                      "Value": "ZASHCHITA_OT_TRAVM_24_09_2020"                    },                    {                      "Name": "ProductId",                      "Value": "1-1IMOBGG9"                    }                  ]                }              ]            }          ],          "ScriptName": "Дебетовая карта.Новая"        }      ]    }  ]}',),
            ('{  "UCPId": "50235802834",  "Offers": [    {      "DefaultOffer": [{        "AttrOffer": [          {            "Name": "Source",            "Value": "CaaS"          },          {            "Name": "Name",            "Value": "Лучшее предложение кредита"          },          {            "Name": "Order",            "Value": "1"          }        ],        "ProductOfferList": [          {            "AttrProdOffer": [              {                "Name": "LineId",                "Value": "1"              },              {                "Name": "UnqProductCode",                "Value": "ZASHCHITA_OT_TRAVM_24_09_2020"              },              {                "Name": "ProductId",                "Value": "1-1IMOBGG6"              },              {                "Name": "RateMax",                "Value": "0.07"              },              {                "Name": "RateMin",                "Value": "0.01"              },              {                "Name": "Marat",                "Value": "100"              }                           ]          }        ]      }],      "Step": "KDV",      "UserRole": "MP",      "ScriptOffers": [        {          "OfferList": [            {              "AttrOffer": [                {                  "Name": "Source",                  "Value": "CaaS"                },                {                  "Name": "Name",                  "Value": "Лучшее предложение кредита"                },                {                  "Name": "Order",                  "Value": "1"                }              ],              "ProductOfferList": [                {                  "AttrProdOffer": [                    {                      "Name": "LineId",                      "Value": "1"                    },                    {                      "Name": "UnqProductCode",                      "Value": "ZASHCHITA_OT_TRAVM_24_09_2020"                    },                    {                      "Name": "ProductId",                      "Value": "1-1IMOBGG6"                    }                  ]                }              ]            }          ],          "ScriptName": "Дебетовая карта.Новая"        }      ]    }  ]}',),
            ('{  "UCPId": "50235802834",  "Offers": [    {      "DefaultOffer": [{        "AttrOffer": [          {            "Name": "Source",            "Value": "CaaS"          },          {            "Name": "Name",            "Value": "Лучшее предложение кредита"          },          {            "Name": "Order",            "Value": "1"          }        ],        "ProductOfferList": [          {            "AttrProdOffer": [              {                "Name": "LineId",                "Value": "1"              },              {                "Name": "UnqProductCode",                "Value": "ZASHCHITA_OT_TRAVM_24_09_2020"              },              {                "Name": "ProductId",                "Value": "1-1IMOBGG6"              },              {                "Name": "RateMax",                "Value": "0.07"              },              {                "Name": "RateMin",                "Value": "0.01"              },              {                "Name": "Marat",                "Value": "100"              }                           ]          }        ]      }],      "Step": "Presentation",      "UserRole": "DSA",      "ScriptOffers": [        {          "OfferList": [            {              "AttrOffer": [                {                  "Name": "Source",                  "Value": "CaaS"                },                {                  "Name": "Name",                  "Value": "Лучшее предложение кредита"                },                {                  "Name": "Order",                  "Value": "1"                }              ],              "ProductOfferList": [                {                  "AttrProdOffer": [                    {                      "Name": "LineId",                      "Value": "1"                    },                    {                      "Name": "UnqProductCode",                      "Value": "ZASHCHITA_OT_TRAVM_24_09_2020"                    },                    {                      "Name": "ProductId",                      "Value": "1-1IMOBGG6"                    }                  ]                }              ]            }          ],          "ScriptName": "Дебетовая карта.Новая"        }      ]    }  ]}',),
    ]
    data_schema = StructType([StructField("json_line", StringType(), True),
                              ])
    df = spark_session.createDataFrame(data=data, schema=data_schema)
    df.write.saveAsTable('{}.{}'.format(schema, name))

    yield schema, name

    # выполнится после выхода из scope-a данной fixture
    spark_session.sql('DROP TABLE {}.{}'.format(schema, name))


@pytest.fixture(scope='function')
def metadata_table(spark_session) -> (str, str):
    """
    Создает синтетическую таблицу с метаданными
    В указанной схеме с указанным название

    После выхода из указанной области действия, таблица удалится
    """
    schema = 'default'
    name = 'metadata'

    data = [('nbo_for_dsa', datetime.now(), 'archive', datetime(1996, 12, 25), datetime.now(), 999)]
    data_schema = StructType([StructField("datamart", StringType(), True),
                              StructField("datamart_finished_dttm", TimestampType(), True),
                              StructField("mode", StringType(), True),
                              StructField("src_ctl_validfrom", TimestampType(), True),
                              StructField("ctl_validfrom", TimestampType(), True),
                              StructField("ctl_loading_id", IntegerType(), True)])

    df = spark_session.createDataFrame(data=data, schema=data_schema)
    df.write.saveAsTable('{}.{}'.format(schema, name))

    yield schema, name

    # выполнится после выхода из scope-a данной fixture
    spark_session.sql('DROP TABLE {}.{}'.format(schema, name))


def test_row_with_metadata_correct():
    RowWithMetadata(datamart="test",
                    datamart_finished_dttm=datetime(2022, 12, 31),
                    mode="test",
                    src_ctl_validfrom=datetime(2022, 12, 31),
                    ctl_validfrom=datetime(2022, 12, 31),
                    ctl_loading_id=1)


def test_row_with_metadata_mistake_one_field():
    """
    тест на создание экземпляра с пропущенным, обязательным параметром
    """
    with pytest.raises(TypeError):
        RowWithMetadata(datamart="test",
                        datamart_finished_dttm=datetime(2022, 12, 31),
                        mode="test",
                        ctl_validfrom=datetime(2022, 12, 31),
                        ctl_loading_id=1)


def test_select_work_mode():
    SelectedClass = select_mode(increment_mode="0",
                                point_date_mode="0",
                                synthetic_mode="1",
                                archive_mode="0",
                                )

    assert SelectedClass is SyntheticMode


def test_select_mode_incorrect():
    with pytest.raises(ValueError) as excinfo:
        select_mode(increment_mode="0",
                    point_date_mode="0",
                    synthetic_mode="0",
                    archive_mode="0",
                    )

    assert "Set one working mode" in str(excinfo.value)


def test_select_mode_incorrect_two_mode():
    with pytest.raises(ValueError) as excinfo:
        select_mode(increment_mode="1",
                    point_date_mode="1",
                    synthetic_mode="0",
                    archive_mode="0",
                    )

    assert "Set one working mode" in str(excinfo.value)


# CONSTRUCTOR
@pytest.mark.skip(reason="work mode not implemented")
def test_increment_mode_constructor(spark_session, synthetic_table):
    schema, name = synthetic_table
    # _ = IncrementMode(spark=spark_session,
    #                   schema=schema,
    #                   table=name,
    #                   synthetic_table='no_important',
    #                   metadata_table='no_important',
    #                   columns_to_kafka=['no', 'important'],
    #                   partition_colum="report_dt",
    #                   field_in_metadata_table='no_important')


@pytest.mark.skip(reason="work mode not implemented")
def test_archive_mode_constructor(spark_session, synthetic_table):
    schema, name = synthetic_table
    # _ = ArchiveMode(spark=spark_session,
    #                 schema=schema,
    #                 table=name,
    #                 synthetic_table='no_important',
    #                 metadata_table='no_important',
    #                 columns_to_kafka=['no', 'important'],
    #                 partition_colum="report_dt",
    #                 field_in_metadata_table='no_important')


@pytest.mark.skip(reason="work mode not implemented")
def test_synthetic_mode_constructor():
    pass
    # _ = SyntheticMode(spark='no_important',
    #                   schema='no_important',
    #                   table='no_important',
    #                   synthetic_table='no_important',
    #                   metadata_table='no_important',
    #                   columns_to_kafka=['no', 'important'],
    #                   partition_colum="no_important",
    #                   field_in_metadata_table='no_important')


@pytest.mark.skip(reason="work mode not implemented")
def test_point_date_mode_constructor():
    pass
    # _ = PointDateMode(spark='no_important',
    #                   schema='no_important',
    #                   table='no_important',
    #                   synthetic_table='no_important',
    #                   metadata_table='no_important',
    #                   columns_to_kafka=['no', 'important'],
    #                   partition_colum="no_important",
    #                   field_in_metadata_table='no_important',
    #                   value_date='2022-01-01')


@pytest.mark.skip(reason="work mode not implemented")
def test_point_date_mode_constructor_miss_value_date():
    """
    тест на пропуск обязательного, для данного режима, параметра
    """
    with pytest.raises(ValueError) as excinfo:
        # _ = PointDateMode(spark='no_important',
        #                   schema='no_important',
        #                   table='no_important',
        #                   synthetic_table='no_important',
        #                   metadata_table='no_important',
        #                   columns_to_kafka=['no', 'important'],
        #                   partition_colum="no_important",
        #                   field_in_metadata_table='no_important')

        assert "Set params 'value_date' for PointDateMode" in str(excinfo.value)


# GET DATA COUNT
@pytest.mark.skip(reason="work mode not implemented")
def test_get_data_n_rows_increment(spark_session, synthetic_table, metadata_table):
    schema, name = synthetic_table
    df = spark_session.table("{}.{}".format(schema, name))

    metadata_schema, metadata_name = metadata_table

    # mode = IncrementMode(spark=spark_session,
    #                      schema=schema,
    #                      table=name,
    #                      synthetic_table='no_important',
    #                      metadata_table=metadata_name,
    #                      columns_to_kafka=df.columns,
    #                      partition_colum="report_dt",
    #                      field_in_metadata_table='src_ctl_validfrom')

    # кол-во строк в последней партиции
    last_partition_val = df.agg({mode.partition_colum: 'max'}).collect()[0]
    last_partition_val = last_partition_val[f'max({mode.partition_colum})']
    synthetic_df_last_partition = df.filter(df.report_dt == last_partition_val)
    expect_rows = synthetic_df_last_partition.count()

    df = mode.get_data_to_kafka()
    fact_rows = df.count()

    assert fact_rows == expect_rows


@pytest.mark.skip(reason="work mode not implemented")
def test_get_data_n_rows_archive(spark_session, synthetic_table):
    schema, name = synthetic_table
    df = spark_session.table("{}.{}".format(schema, name))

    # mode = ArchiveMode(spark=spark_session,
    #                    schema=schema,
    #                    table=name,
    #                    synthetic_table='no_important',
    #                    metadata_table='no_important',
    #                    columns_to_kafka=df.columns,
    #                    partition_colum="report_dt",
    #                    field_in_metadata_table='no_important')

    # кол-во строк в последней партиции
    last_partition_val = df.agg({mode.partition_colum: 'max'}).collect()[0]
    last_partition_val = last_partition_val[f'max({mode.partition_colum})']
    synthetic_df_last_partition = df.filter(df.report_dt == last_partition_val)
    expect_rows = synthetic_df_last_partition.count()

    df = mode.get_data_to_kafka()
    fact_rows = df.count()

    assert fact_rows == expect_rows


@pytest.mark.skip(reason="work mode not implemented")
def test_get_data_n_rows_point_date(spark_session, synthetic_table):
    schema, name = synthetic_table
    df = spark_session.table("{}.{}".format(schema, name))

    testing_value_date = '2021-12-15'
    # mode = PointDateMode(spark=spark_session,
    #                      schema=schema,
    #                      table=name,
    #                      synthetic_table='no_important',
    #                      metadata_table='no_important',
    #                      columns_to_kafka=df.columns,
    #                      partition_colum="report_dt",
    #                      field_in_metadata_table='no_important',
    #                      value_date=testing_value_date)

    synthetic_df_table_filtered = df.filter(df.report_dt == testing_value_date)
    expect_rows = synthetic_df_table_filtered.count()

    df = mode.get_data_to_kafka()
    fact_rows = df.count()

    assert fact_rows == expect_rows


def test_get_data_n_rows_synthetic(spark_session, synthetic_table):
    schema, name = synthetic_table
    df = spark_session.table("{}.{}".format(schema, name))

    mode = SyntheticMode(spark=spark_session,
                         schema=schema,
                         table='no_important',
                         synthetic_table='synthetic',
                         metadata_table='no_important',
                         columns_to_kafka=df.columns,
                         partition_colum="no_important",
                         field_in_metadata_table='no_important')

    expect_rows = df.count()

    df = mode.get_data_to_kafka()
    fact_rows = df.count()

    assert fact_rows == expect_rows


# UPDATE METADATE
def test_update_metadata_synthetic(spark_session, metadata_table):
    metadata_schema, metadata_name = metadata_table

    mode = SyntheticMode(spark='no_important',
                         schema='no_important',
                         table='no_important',
                         synthetic_table='no_important',
                         metadata_table=metadata_name,
                         columns_to_kafka=['no', 'important'],
                         partition_colum="no_important",
                         field_in_metadata_table='no_important')

    row_with_metadata = RowWithMetadata(datamart='no_important',
                                        datamart_finished_dttm=datetime.now(),
                                        mode=str(mode),
                                        src_ctl_validfrom=mode.src_ctl_validfrom,
                                        ctl_validfrom=datetime.now(),
                                        ctl_loading_id=999)
    df_metadata = spark_session.table(f"{metadata_schema}.{metadata_name}")

    rows_before_updating = df_metadata.count()
    mode.update_metadata_table(row_with_metadata)
    rows_after_updating = df_metadata.count()

    # в данном режиме метаинформация не должна обновляться
    assert rows_after_updating == rows_before_updating


@pytest.mark.skip(reason="work mode not implemented")
def test_update_metadata_increment(spark_session, synthetic_table, metadata_table):
    schema, name = synthetic_table
    df = spark_session.table("{}.{}".format(schema, name))

    metadata_schema, metadata_name = metadata_table

    # mode = IncrementMode(spark=spark_session,
    #                      schema=schema,
    #                      table=name,
    #                      synthetic_table='no_important',
    #                      metadata_table=metadata_name,
    #                      columns_to_kafka=df.columns,
    #                      partition_colum="report_dt",
    #                      field_in_metadata_table='src_ctl_validfrom')

    row_with_metadata = RowWithMetadata(datamart='no_important',
                                        datamart_finished_dttm=datetime.now(),
                                        mode=str(mode),
                                        src_ctl_validfrom=mode.src_ctl_validfrom,
                                        ctl_validfrom=datetime.now(),
                                        ctl_loading_id=999)

    df_metadata = spark_session.table(f"{metadata_schema}.{metadata_name}")

    rows_before_updating = df_metadata.count()
    mode.update_metadata_table(row_with_metadata)
    rows_after_updating = df_metadata.count()

    assert rows_after_updating == rows_before_updating + 1


@pytest.mark.skip(reason="work mode not implemented")
def test_update_metadata_archive(spark_session, synthetic_table, metadata_table):
    schema, name = synthetic_table
    df = spark_session.table("{}.{}".format(schema, name))

    metadata_schema, metadata_name = metadata_table

    # mode = ArchiveMode(spark=spark_session,
    #                    schema=schema,
    #                    table=name,
    #                    synthetic_table='no_important',
    #                    metadata_table=metadata_name,
    #                    columns_to_kafka=df.columns,
    #                    partition_colum="report_dt",
    #                    field_in_metadata_table='src_ctl_validfrom',
    #                    value_date=2)

    row_with_metadata = RowWithMetadata(datamart='no_important',
                                        datamart_finished_dttm=datetime.now(),
                                        mode=str(mode),
                                        src_ctl_validfrom=mode.src_ctl_validfrom,
                                        ctl_validfrom=datetime.now(),
                                        ctl_loading_id=999)
    mode.update_metadata_table(row_with_metadata)

    df_metadata = spark_session.table(f"{metadata_schema}.{metadata_name}")
    rows_after_updating = df_metadata.count()

    assert rows_after_updating == 1


@pytest.mark.skip(reason="work mode not implemented")
def test_update_metadata_point_date(spark_session, synthetic_table, metadata_table):
    schema, name = synthetic_table
    df = spark_session.table("{}.{}".format(schema, name))

    metadata_schema, metadata_name = metadata_table

    testing_value_date = '2021-12-15'
    # mode = PointDateMode(spark=spark_session,
    #                      schema=schema,
    #                      table=name,
    #                      synthetic_table='no_important',
    #                      metadata_table=metadata_name,
    #                      columns_to_kafka=df.columns,
    #                      partition_colum="report_dt",
    #                      field_in_metadata_table='no_important',
    #                      value_date=testing_value_date)

    row_with_metadata = RowWithMetadata(datamart='no_important',
                                        datamart_finished_dttm=datetime.now(),
                                        mode=str(mode),
                                        src_ctl_validfrom=mode.src_ctl_validfrom,
                                        ctl_validfrom=datetime.now(),
                                        ctl_loading_id=999)

    df_metadata = spark_session.table(f"{metadata_schema}.{metadata_name}")

    rows_before_updating = df_metadata.count()
    mode.update_metadata_table(row_with_metadata)
    rows_after_updating = df_metadata.count()

    assert rows_after_updating == rows_before_updating + 1


@pytest.mark.skip(reason="work mode not implemented")
def test_incorrect_date_for_point_date_mode():
    with pytest.raises(ValueError) as excinfo:
        testing_value_date = '0'
        # _ = PointDateMode(spark='no_important',
        #                   schema='no_important',
        #                   table='no_important',
        #                   synthetic_table='no_important',
        #                   metadata_table='no_important',
        #                   columns_to_kafka=['no'],
        #                   partition_colum="report_dt",
        #                   field_in_metadata_table='no_important',
        #                   value_date=testing_value_date)

        assert "does not match format" in str(excinfo.value)


def test_synthetic_str_repr():
    """
    проверка того, правильно ли работает преобразование класса к строке
    """
    mode = SyntheticMode(spark='no_important',
                         schema='no_important',
                         table='no_important',
                         synthetic_table='no_important',
                         metadata_table='no_important',
                         columns_to_kafka=['no', 'important'],
                         partition_colum="no_important",
                         field_in_metadata_table='no_important')
    assert str(mode) == 'SyntheticMode'
