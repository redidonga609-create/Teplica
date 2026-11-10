import kotlinx.coroutines.*
import kotlin.random.Random
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.SendChannel
import kotlinx.coroutines.flow.*

data class SensorReading(val sensorId: String, val value: Double, val timestamp: Long)
suspend fun emitSensorData(sensorId: String, channel: SendChannel<SensorReading>) = coroutineScope {
    launch {
        for (i in 1..10)
        {
            delay(500)
            val pokazania = SensorReading(
                sensorId,
                Random.nextDouble(15.0, 30.0),
                timestamp = System.currentTimeMillis()
            )
            channel.send(pokazania)
        }
    }
}
suspend fun main() = coroutineScope{
    val channel = Channel<SensorReading>()
    val job = listOf( // Стартуют сразу же как только вызвали
    launch {
        emitSensorData("TEMP", channel)
    },
    launch {
        emitSensorData("HUM", channel)
    },
    launch {
        emitSensorData( "CO2", channel)
    }
    )
    launch {
        job.joinAll()
        channel.close()
    }
    for (data in channel){
        println(data)
    }

    val flowMethod = sensorFlow("TEMP") // Ещё ничего не делает, пока не вызовем fold или toList
    println(flowMethod.toList())
    val (sum, count) = flowMethod.fold(Pair(0.0, 0)) {(accSum, accCount), reading ->
        Pair(accSum+reading.value, accCount + 1)
    }
    println()
    if (count > 0) {
        println("Результат агрегации:")
        println("  Сумма значений : ${"%.2f".format(sum)}")
        println("  Кол-во элементов: $count")
        println("  Среднее        : ${"%.2f".format(sum / count)}")
    } else {
        println("Поток не выдал ни одного элемента (все были отфильтрованы).")
    }
}
fun sensorFlow(sensorId: String): Flow<SensorReading> = flow{
    repeat(10){
        delay(300)
        val pokazania = SensorReading(
            sensorId,
            Random.nextDouble(15.0, 30.0),
            timestamp = System.currentTimeMillis()
        )
        emit(pokazania)

    }
}.filter { it.value > 20 } // Отбираем тольео те, чьё занчение value больше 20
    .map { it.copy(value = it.value*1.1) } // Преобразовываем значение value
    .take(5). // Берем только 5 первых элементов, которые подойдут под фильтр
    onEach { println("Обработано: $it")  } // К каждому элементу применяем println