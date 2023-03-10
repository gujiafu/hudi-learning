package cn.itcast.hudi.test

import java.util.{Calendar, Date}

import org.apache.commons.lang3.time.FastDateFormat

/**
 * 将日期转换星期，例如输入：2021-10-10 -> 星期日
 */
object DayWeekTest {
	
	def main(args: Array[String]): Unit = {
		
		val dateStr: String = "2021-09-10"
		
		val format: FastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd")
		val calendar: Calendar = Calendar.getInstance();
		
		val date: Date = format.parse(dateStr)
		calendar.setTime(date)
		
		val dayWeek = calendar.get(Calendar.DAY_OF_WEEK) match {
			case 1 => "星期日"
			case 2 => "星期一"
			case 3 => "星期二"
			case 4 => "星期三"
			case 5 => "星期四"
			case 6 => "星期五"
			case 7 => "星期六"
		}
		println(dayWeek)
	}
	
}
