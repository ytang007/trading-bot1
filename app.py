        elif alert_type == "HOLD_OVERNIGHT":
            subject = f"HOLD OVERNIGHT - {symbol}"
            body = (
                f"HOLD OVERNIGHT\n\n"
                f"Symbol: {symbol}\n"
                f"Current price: {price}\n"
                f"Time: {alert_time}\n\n"
                f"Late-day strength conditions passed.\n"
                f"This position qualifies as an overnight hold.\n"
            )
            enqueue_email(subject, body)

        elif alert_type == "HOLD_OVER_WEEKEND":
            subject = f"HOLD OVER WEEKEND - {symbol}"
            body = (
                f"HOLD OVER WEEKEND\n\n"
                f"Symbol: {symbol}\n"
                f"Current price: {price}\n"
                f"Time: {alert_time}\n\n"
                f"Friday strength conditions passed.\n"
                f"This position qualifies as a weekend hold.\n"
                f"Weekend gap/news risk still exists, so keep size disciplined.\n"
            )
            enqueue_email(subject, body)

        elif alert_type == "SELL_BEFORE_WEEKEND":
            subject = f"SELL BEFORE WEEKEND - {symbol}"
            body = (
                f"SELL BEFORE WEEKEND\n\n"
                f"Symbol: {symbol}\n"
                f"Current price: {price}\n"
                f"Time: {alert_time}\n\n"
                f"This position does not meet the weekend-hold criteria.\n"
                f"Close before the weekend.\n"
            )
            enqueue_email(subject, body)
