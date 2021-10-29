# -*- coding: utf-8 -*-
import logging
import time

from apscheduler.schedulers.background import BackgroundScheduler

from examples.reports import stocks_with_info
from examples.utils import add_to_eastmoney
from zvt import init_log, zvt_config
from zvt.api import get_top_volume_entities
from zvt.api.kdata import get_latest_kdata_date
from zvt.contract import AdjustType
from zvt.contract.api import get_entities
from zvt.domain import Stock
from zvt.factors import VolumeUpMaFactor
from zvt.factors.target_selector import TargetSelector, SelectMode
from zvt.informer.informer import EmailInformer
from zvt.utils import next_date

logger = logging.getLogger(__name__)

sched = BackgroundScheduler()


@sched.scheduled_job('cron', hour=19, minute=0, day_of_week='mon-fri')
def report_vol_up():
    while True:
        error_count = 0
        email_action = EmailInformer()

        try:
            target_date = get_latest_kdata_date(entity_type='stock', adjust_type=AdjustType.hfq)

            start_timestamp = next_date(target_date, -30)
            # 成交量
            vol_df = get_top_volume_entities(entity_type='stock',
                                             start_timestamp=start_timestamp,
                                             end_timestamp=target_date,
                                             pct=0.4)
            current_entity_pool = vol_df.index.tolist()

            # 计算均线
            start = '2019-01-01'
            my_selector = TargetSelector(start_timestamp=start, end_timestamp=target_date,
                                         select_mode=SelectMode.condition_or)
            # add the factors
            tech_factor = VolumeUpMaFactor(entity_ids=current_entity_pool, start_timestamp=start,
                                           end_timestamp=target_date,
                                           windows=[120, 250], over_mode='or', up_intervals=50,
                                           turnover_threshold=400000000)

            my_selector.add_factor(tech_factor)

            my_selector.run()

            long_stocks = my_selector.get_open_long_targets(timestamp=target_date)

            msg = 'no targets'

            if long_stocks:
                stocks = get_entities(provider='joinquant', entity_schema=Stock, entity_ids=long_stocks,
                                      return_type='domain')
                # add them to eastmoney
                try:
                    codes = [stock.code for stock in stocks]
                    add_to_eastmoney(codes=codes, entity_type='stock', group='tech')
                except Exception as e:
                    email_action.send_message(zvt_config['email_username'], f'report_vol_up error',
                                              'report_vol_up error:{}'.format(e))

                infos = stocks_with_info(stocks)
                msg = '\n'.join(infos) + '\n'

            logger.info(msg)

            email_action.send_message(zvt_config['email_username'], f'{target_date} 改进版放量突破(半)年线选股结果', msg)

            break
        except Exception as e:
            logger.exception('report_vol_up error:{}'.format(e))
            time.sleep(60 * 3)
            error_count = error_count + 1
            if error_count == 10:
                email_action.send_message(zvt_config['email_username'], f'report_vol_up error',
                                          'report_vol_up error:{}'.format(e))


if __name__ == '__main__':
    init_log('report_vol_up.log')

    report_vol_up()

    sched.start()

    sched._thread.join()
