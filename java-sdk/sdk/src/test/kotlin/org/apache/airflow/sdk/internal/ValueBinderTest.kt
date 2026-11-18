/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.airflow.sdk.internal

import com.fasterxml.jackson.annotation.JsonAlias
import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.annotation.JsonMerge
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.JsonSetter
import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.fasterxml.jackson.annotation.JsonUnwrapped
import com.fasterxml.jackson.annotation.Nulls
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder
import com.fasterxml.jackson.databind.deser.std.StdDeserializer
import com.fasterxml.jackson.databind.util.StdConverter
import org.apache.airflow.sdk.ArgName
import org.apache.airflow.sdk.TypeRef
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import java.lang.reflect.Type
import java.math.BigDecimal
import java.math.BigInteger
import java.time.OffsetDateTime
import java.time.ZonedDateTime
import java.util.concurrent.atomic.AtomicReference

private class Stats {
  @JvmField
  var rowCount: Long = 0

  @JvmField
  var threshold: Double = 0.9

  @JvmField
  var counts: IntArray? = null
}

private class Report {
  @JvmField
  var stats: Stats? = null

  @JvmField
  var history: List<Stats>? = null

  @JvmField
  var byRegion: Map<String, Stats>? = null

  @JvmField
  var current: AtomicReference<Stats>? = null
}

private class Dated {
  @JvmField
  var runAt: OffsetDateTime? = null

  @JvmField
  var zoned: ZonedDateTime? = null
}

private class Pinned {
  @JvmField
  @ArgName("Region_Code")
  var region: String? = null
}

private class InheritsPinned : PinnedBase()

private open class PinnedBase {
  @JvmField
  @ArgName("region_code")
  var region: String? = null
}

private class PinnedWithConstant {
  @JvmField
  @ArgName("regionKey")
  var region: String? = null

  companion object {
    @JvmField
    val regionKey = "not a field that a key can set"
  }
}

private class PinnedNameOfAnotherField {
  @JvmField
  @ArgName("other")
  var pinned: String? = null

  @JvmField
  var other: String? = null
}

@JsonDeserialize(using = ReadsKeysAsWritten::class)
private class CustomDeserialized {
  @JvmField
  var rowCount: Long = 0
}

private class ReadsKeysAsWritten : StdDeserializer<CustomDeserialized>(CustomDeserialized::class.java) {
  override fun deserialize(
    parser: JsonParser,
    context: DeserializationContext,
  ): CustomDeserialized {
    val node: JsonNode = parser.readValueAsTree()
    return CustomDeserialized().also { it.rowCount = node.get("row_count").asLong() }
  }
}

private class Window {
  @JvmField
  var startTime: String? = null
}

private class ReadsWindowAsWritten : StdDeserializer<Window>(Window::class.java) {
  override fun deserialize(
    parser: JsonParser,
    context: DeserializationContext,
  ): Window {
    val node: JsonNode = parser.readValueAsTree()
    return Window().also { it.startTime = node.get("start_time")?.asText() }
  }
}

private class WindowFromKeysAsWritten : StdConverter<JsonNode, Window>() {
  override fun convert(value: JsonNode): Window = Window().also { it.startTime = value.get("start_time")?.asText() }
}

private class Schedule {
  @field:JsonDeserialize(using = ReadsWindowAsWritten::class)
  @JvmField
  var window: Window? = null

  @field:JsonDeserialize(contentUsing = ReadsWindowAsWritten::class)
  @JvmField
  var windows: List<Window>? = null

  @field:JsonDeserialize(converter = WindowFromKeysAsWritten::class)
  @JvmField
  var converted: Window? = null

  @field:JsonDeserialize(contentConverter = WindowFromKeysAsWritten::class)
  @JvmField
  var convertedWindows: List<Window>? = null

  @set:JsonDeserialize(using = ReadsWindowAsWritten::class)
  var fromSetter: Window? = null

  @field:JsonDeserialize(contentUsing = ReadsWindowAsWritten::class)
  @JvmField
  var windowsByName: Map<String, Window>? = null

  @field:JsonDeserialize(contentUsing = ReadsWindowAsWritten::class)
  @JvmField
  var windowRef: AtomicReference<Window>? = null
}

private class ReadsWindowsAsWritten : StdDeserializer<Windows>(Windows::class.java) {
  override fun deserialize(
    parser: JsonParser,
    context: DeserializationContext,
  ): Windows {
    val node: JsonNode = parser.readValueAsTree()
    return Windows().also { windows ->
      node.forEach { element -> windows += Window().also { it.startTime = element.get("start_time")?.asText() } }
    }
  }
}

@JsonDeserialize(using = ReadsWindowsAsWritten::class)
private class Windows : ArrayList<Window>()

@JsonDeserialize(contentUsing = ReadsWindowAsWritten::class)
private class WindowList : ArrayList<Window>()

private class History {
  @field:JsonDeserialize(`as` = ArrayList::class)
  @JvmField
  var history: List<Stats>? = null
}

private class Aliased {
  @field:JsonAlias("user_id")
  @JvmField
  var owner: String? = null

  @JvmField
  var userId: String? = null
}

private class AliasedTable {
  @field:JsonAlias("tbl")
  @JvmField
  var targetTable: String? = null
}

private class MinusOneOnNull : StdDeserializer<Int>(Int::class.java) {
  override fun deserialize(
    parser: JsonParser,
    context: DeserializationContext,
  ): Int = parser.intValue

  override fun getNullValue(context: DeserializationContext): Int = -1
}

private class NullWithOwnValue {
  @field:JsonDeserialize(using = MinusOneOnNull::class)
  @JvmField
  var retries: Int = 3
}

private class MergedRows {
  @field:JsonMerge
  @JvmField
  var stats: Stats = Stats().also { it.threshold = 0.5 }
}

private class UnwrappedRows {
  @JvmField
  var name: String? = null

  @field:JsonUnwrapped
  @JvmField
  var stats: Stats = Stats()
}

private class MisplacedContentUsing {
  @field:JsonDeserialize(contentUsing = ReadsWindowAsWritten::class)
  @JvmField
  var window: Window? = null
}

private class NeedsArgument(
  private val startTime: String,
) : StdConverter<JsonNode, Window>() {
  override fun convert(value: JsonNode): Window = Window().also { it.startTime = startTime }
}

private class BrokenConverter {
  @field:JsonDeserialize(converter = NeedsArgument::class)
  @JvmField
  var window: Window? = null
}

@JsonDeserialize(converter = SlotFromKeysAsWritten::class)
private class Slot {
  @JvmField
  var startTime: String? = null
}

private class SlotFromKeysAsWritten : StdConverter<JsonNode, Slot>() {
  override fun convert(value: JsonNode): Slot = Slot().also { it.startTime = value.get("start_time")?.asText() }
}

private class DelegatedSlot
  @JsonCreator(mode = JsonCreator.Mode.DELEGATING)
  constructor(
    keys: Map<String, Any?>,
  ) {
    @JvmField
    val startTime = keys["start_time"] as String?
  }

@JsonDeserialize(builder = BuiltWindow.Builder::class)
private class BuiltWindow private constructor(
  val startTime: String?,
) {
  @JsonPOJOBuilder(withPrefix = "")
  class Builder {
    private var startTime: String? = null

    @JsonProperty("start_time")
    fun startTime(value: String?) = apply { startTime = value }

    fun build() = BuiltWindow(startTime)
  }
}

private class HasSlots {
  @JvmField
  var slot: Slot? = null

  @JvmField
  var slots: List<Slot>? = null

  @JvmField
  var delegated: DelegatedSlot? = null
}

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "kind")
@JsonSubTypes(
  JsonSubTypes.Type(value = DailyRun::class, name = "daily"),
  JsonSubTypes.Type(value = CustomRun::class, name = "custom"),
)
private abstract class ScheduledRun {
  @JvmField
  var rowCount: Long = 0
}

private class DailyRun : ScheduledRun()

@JsonDeserialize(using = ReadsCustomRunAsWritten::class)
private class CustomRun : ScheduledRun()

private class ReadsCustomRunAsWritten : StdDeserializer<CustomRun>(CustomRun::class.java) {
  override fun deserialize(
    parser: JsonParser,
    context: DeserializationContext,
  ): CustomRun {
    val node: JsonNode = parser.readValueAsTree()
    return CustomRun().also { it.rowCount = node.get("row_count")?.asLong() ?: -1 }
  }
}

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "kind")
@JsonSubTypes(JsonSubTypes.Type(value = CustomShift::class, name = "custom"))
private open class Shift {
  @JvmField
  var rowCount: Long = 0
}

@JsonDeserialize(using = ReadsCustomShiftAsWritten::class)
private class CustomShift : Shift()

private class ReadsCustomShiftAsWritten : StdDeserializer<CustomShift>(CustomShift::class.java) {
  override fun deserialize(
    parser: JsonParser,
    context: DeserializationContext,
  ): CustomShift {
    val node: JsonNode = parser.readValueAsTree()
    return CustomShift().also { it.rowCount = node.get("row_count")?.asLong() ?: -1 }
  }
}

private open class Tally {
  @JvmField
  var rowCount: Long = 0
}

@JsonDeserialize(using = ReadsCustomTallyAsWritten::class)
private class CustomTally : Tally()

private class ReadsCustomTallyAsWritten : StdDeserializer<CustomTally>(CustomTally::class.java) {
  override fun deserialize(
    parser: JsonParser,
    context: DeserializationContext,
  ): CustomTally {
    val node: JsonNode = parser.readValueAsTree()
    return CustomTally().also { it.rowCount = node.get("row_count")?.asLong() ?: -1 }
  }
}

private class Crew {
  @field:JsonDeserialize(`as` = CustomTally::class)
  @JvmField
  var picked: Tally? = null

  @field:JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "kind")
  @field:JsonSubTypes(JsonSubTypes.Type(value = CustomTally::class, name = "custom"))
  @JvmField
  var typed: Tally? = null

  @field:JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "kind")
  @field:JsonSubTypes(JsonSubTypes.Type(value = CustomTally::class, name = "custom"))
  @JvmField
  var typedList: List<Tally>? = null

  @field:JsonDeserialize(contentAs = CustomTally::class)
  @JvmField
  var contentPicked: List<Tally>? = null
}

@JsonDeserialize(contentAs = CustomTally::class)
private class Tallies : ArrayList<Tally>()

@JsonDeserialize(`as` = PlainLog::class)
private open class LogBase {
  @JvmField
  var rowCount: Long = 0
}

private class PlainLog : LogBase()

private class DuplicateProperty {
  @field:JsonProperty("label")
  @JvmField
  var first: String? = null

  @field:JsonProperty("label")
  @JvmField
  var second: String? = null
}

private class SkipsNulls {
  @field:JsonSetter(nulls = Nulls.SKIP)
  @JvmField
  var retries: Int = 3

  @field:JsonSetter(nulls = Nulls.AS_EMPTY)
  @JvmField
  var limit: Int = 7
}

private class SetterSkipsNull {
  @set:JsonSetter(nulls = Nulls.SKIP)
  var retries: Int = 3
}

private class CreatorSkipsNull
  @JsonCreator
  constructor(
    @JsonProperty("retries") @param:JsonSetter(nulls = Nulls.SKIP) val retries: Int,
  )

private class CreatorAsEmpty
  @JsonCreator
  constructor(
    @JsonProperty("retries") @param:JsonSetter(nulls = Nulls.AS_EMPTY) val retries: Int,
  )

private class FoldClash {
  @JvmField
  var fooBar: String? = null

  @JvmField
  var foobar: String? = null
}

private class PinnedFoldClash {
  @JvmField
  @ArgName("foo_bar")
  var pinned: String? = null

  @JvmField
  var fooBar: String? = null
}

@JsonIgnoreProperties(ignoreUnknown = true)
private class IgnoresUnknownKeys {
  @JvmField
  var rowCount: Long = 0
}

// Stands in for a record, as CreatorConf in ContextTest does.
private class CreatorStats
  @JsonCreator
  constructor(
    @JsonProperty("rowCount") val rowCount: Long,
    @JsonProperty("label") val label: String?,
  )

private abstract class Payload

private class ClickPayload : Payload() {
  @JvmField
  var xPos: Int = 0
}

private class ClickEvent {
  @JvmField
  var payloadType: String? = null

  @field:JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.EXTERNAL_PROPERTY, property = "payloadType")
  @field:JsonSubTypes(JsonSubTypes.Type(value = ClickPayload::class, name = "click"))
  @JvmField
  var eventPayload: Payload? = null
}

private class EventHolder {
  @field:JsonIgnoreProperties("source")
  @JvmField
  var event: ClickEvent? = null
}

private class ClickEventByIdName {
  @JvmField
  var payloadType: String? = null

  @field:JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.EXTERNAL_PROPERTY, property = "payload_type")
  @field:JsonSubTypes(JsonSubTypes.Type(value = ClickPayload::class, name = "click"))
  @JvmField
  var payload: Payload? = null
}

private class Customer {
  @JvmField
  var id: String? = null
}

private class Order {
  @JvmField
  var customerId: String? = null

  @field:JsonUnwrapped(prefix = "customer_")
  @JvmField
  var customer: Customer = Customer()
}

private class ShadowedRows {
  @JvmField
  var rowCount: Long = 0

  @field:JsonUnwrapped
  @JvmField
  var stats: Stats = Stats()
}

private class Chain {
  @JvmField
  var name: String? = null

  @field:JsonUnwrapped(prefix = "next_")
  @JvmField
  var next: Chain? = null
}

private class ChainWithNextName {
  @JvmField
  var name: String? = null

  @JvmField
  var nextName: String? = null

  @field:JsonUnwrapped(prefix = "next_")
  @JvmField
  var next: ChainWithNextName? = null
}

private class UnwrappedStatsCount {
  @JvmField
  var statscount: Long = 0

  @field:JsonUnwrapped
  @JvmField
  var statsCount: Stats = Stats()
}

// Jackson cannot unwrap a `Map`, so `extra` stays an ordinary field and takes
// the key `extra`.
private class UnwrappedMapClash {
  @field:JsonUnwrapped
  @JvmField
  var extra: Map<String, Any?>? = null

  @JvmField
  @ArgName("Extra")
  var label: String? = null
}

private class Location {
  @JvmField
  @ArgName("ID")
  var id: String? = null

  @JvmField
  var cityName: String? = null
}

private class Trip {
  @field:JsonUnwrapped(prefix = "src_")
  @JvmField
  var src: Location = Location()
}

private class FailsOnNull {
  @field:JsonSetter(nulls = Nulls.FAIL)
  @JvmField
  var retries: Int = 3
}

private open class PinnedInBase {
  @JvmField
  @ArgName("label")
  var name: String? = null
}

private class LabelInSub : PinnedInBase() {
  @JvmField
  var label: String? = null
}

private open class LabelBase {
  @JvmField
  var label: String? = null
}

private class PinnedOverLabel : LabelBase() {
  @JvmField
  @ArgName("label")
  var name: String? = null
}

private fun assertFailsWith(
  value: Any?,
  type: Type,
  expectedInMessage: String,
) {
  val error = assertThrows(IllegalArgumentException::class.java) { ArgValues.decode(value, type) }

  val message = error.message.orEmpty()
  assertTrue(message.startsWith("Cannot convert a task argument to ${type.typeName}: ")) { message }
  assertTrue(message.contains(expectedInMessage)) { message }
}

internal class ValueBinderTest {
  @Test
  @DisplayName("Should fail a task argument on a key that no field of the class declares")
  fun shouldFailOnKeyNoFieldDeclares() {
    assertFailsWith(mapOf("thresh" to 0.5), Stats::class.java, "Unrecognized field \"thresh\"")
    assertFailsWith(mapOf("stats" to mapOf("thresh" to 0.5)), Report::class.java, "Unrecognized field \"thresh\"")
  }

  @Test
  @DisplayName("Should fail a task argument on a key that loses to a key with the exact name of the field")
  fun shouldFailOnKeyThatLosesToExactKey() {
    assertFailsWith(mapOf("rowCount" to 1L, "ROW_COUNT" to 2L), Stats::class.java, "Unrecognized field \"ROW_COUNT\"")
  }

  @Test
  @DisplayName("Should let a class accept unknown keys with Jackson's JsonIgnoreProperties")
  fun shouldHonorJsonIgnoreProperties() {
    val stats = ArgValues.decode(mapOf("row_count" to 5L, "source" to "s3"), IgnoresUnknownKeys::class.java)

    assertEquals(5L, (stats as IgnoresUnknownKeys).rowCount)
  }

  @Test
  @DisplayName("Should keep the offset of a date-time in a task argument")
  fun shouldKeepOffsetOfDateTime() {
    val written = "2024-01-31T02:00+08:00"

    assertEquals(OffsetDateTime.parse(written), ArgValues.decode(written, OffsetDateTime::class.java))
    val dated = ArgValues.decode(mapOf("run_at" to written, "zoned" to written), Dated::class.java) as Dated
    assertEquals(OffsetDateTime.parse(written), dated.runAt)
    assertEquals(ZonedDateTime.parse(written), dated.zoned)
  }

  @Test
  @DisplayName("Should bind a pinned field only from its pinned name, as written")
  fun shouldBindPinnedFieldOnlyFromPinnedName() {
    assertEquals("emea", (ArgValues.decode(mapOf("Region_Code" to "emea"), Pinned::class.java) as Pinned).region)

    listOf("region", "regionCode", "region_code", "REGION_CODE").forEach { key ->
      assertFailsWith(mapOf(key to "emea"), Pinned::class.java, "Unrecognized field \"$key\"")
    }
  }

  @Test
  @DisplayName("Should not fold onto a pinned field that the class inherits")
  fun shouldNotFoldOntoInheritedPinnedField() {
    assertEquals("emea", (ArgValues.decode(mapOf("region_code" to "emea"), InheritsPinned::class.java) as PinnedBase).region)

    assertFailsWith(mapOf("REGION_CODE" to "emea"), InheritsPinned::class.java, "Unrecognized field \"REGION_CODE\"")
  }

  @Test
  @DisplayName("Should reject a pinned name that another field has, naming the class of each field")
  fun shouldRejectPinnedNameOfAnotherField() {
    listOf(
      PinnedNameOfAnotherField::class.java to
        "Fields PinnedNameOfAnotherField.pinned and PinnedNameOfAnotherField.other both claim the name 'other', " +
        "so the SDK cannot tell which one a key should go to. Rename one of them.",
      LabelInSub::class.java to
        "Fields PinnedInBase.name and LabelInSub.label both claim the name 'label', so the SDK cannot tell which " +
        "one a key should go to. Rename one of them.",
      PinnedOverLabel::class.java to
        "Fields PinnedOverLabel.name and LabelBase.label both claim the name 'label', so the SDK cannot tell which " +
        "one a key should go to. Rename one of them.",
    ).forEach { (type, expected) ->
      val error = assertThrows(IllegalArgumentException::class.java) { ArgValues.decode(mapOf("other" to "x"), type) }

      assertEquals(expected, error.message)
    }
  }

  @Test
  @DisplayName("Should not compare a pinned name with the name of a static field")
  fun shouldIgnoreStaticFieldWhenCheckingPinnedName() {
    val pinned = ArgValues.decode(mapOf("regionKey" to "emea"), PinnedWithConstant::class.java) as PinnedWithConstant

    assertEquals("emea", pinned.region)
  }

  @Test
  @DisplayName("Should give a custom deserializer the keys as written")
  fun shouldGiveCustomDeserializerKeysAsWritten() {
    val value = ArgValues.decode(mapOf("row_count" to 5L), CustomDeserialized::class.java) as CustomDeserialized

    assertEquals(5L, value.rowCount)
  }

  @Test
  @DisplayName("Should give the keys as written to a field that has its own deserializer or converter")
  fun shouldGiveFieldDeserializerKeysAsWritten() {
    val keys = mapOf("start_time" to "09:00")
    val schedule =
      ArgValues.decode(
        mapOf(
          "window" to keys,
          "windows" to listOf(keys),
          "converted" to keys,
          "converted_windows" to listOf(keys),
          "from_setter" to keys,
          "windows_by_name" to mapOf("morning" to keys),
          "window_ref" to keys,
        ),
        Schedule::class.java,
      ) as Schedule

    assertEquals("09:00", schedule.window?.startTime)
    assertEquals("09:00", schedule.windows?.single()?.startTime)
    assertEquals("09:00", schedule.converted?.startTime)
    assertEquals("09:00", schedule.convertedWindows?.single()?.startTime)
    assertEquals("09:00", schedule.fromSetter?.startTime)
    assertEquals("09:00", schedule.windowsByName?.get("morning")?.startTime)
    assertEquals("09:00", schedule.windowRef?.get()?.startTime)
  }

  @Test
  @DisplayName("Should match the keys inside a field whose content annotation Jackson ignores")
  fun shouldMatchKeysWhenJacksonIgnoresContentAnnotation() {
    val value = ArgValues.decode(mapOf("window" to mapOf("START_TIME" to "08:00")), MisplacedContentUsing::class.java)

    assertEquals("08:00", (value as MisplacedContentUsing).window?.startTime)
  }

  @Test
  @DisplayName("Should give the keys as written to a class that a converter or a delegating creator reads, at any depth")
  fun shouldGiveClassReaderKeysAsWritten() {
    val keys = mapOf("start_time" to "09:00")

    assertEquals("09:00", (ArgValues.decode(keys, Slot::class.java) as Slot).startTime)
    assertEquals("09:00", (ArgValues.decode(keys, DelegatedSlot::class.java) as DelegatedSlot).startTime)
    val nested = ArgValues.decode(mapOf("slot" to keys, "slots" to listOf(keys), "delegated" to keys), HasSlots::class.java)
    assertEquals("09:00", (nested as HasSlots).slot?.startTime)
    assertEquals("09:00", nested.slots?.single()?.startTime)
    assertEquals("09:00", nested.delegated?.startTime)
  }

  @Test
  @DisplayName("Should match keys to the names that a builder takes")
  fun shouldMatchKeysForBuilder() {
    listOf("start_time", "START_TIME", "startTime").forEach { key ->
      assertEquals("09:00", (ArgValues.decode(mapOf(key to "09:00"), BuiltWindow::class.java) as BuiltWindow).startTime) { key }
    }
  }

  @Test
  @DisplayName("Should give the keys as written to the elements of a list class with its own Jackson annotation")
  fun shouldGiveKeysAsWrittenInsideAnnotatedListClass() {
    val elements = listOf(mapOf("start_time" to "09:00"))

    assertEquals("09:00", (ArgValues.decode(elements, Windows::class.java) as Windows).single().startTime)
    assertEquals("09:00", (ArgValues.decode(elements, WindowList::class.java) as WindowList).single().startTime)
    val tallies = ArgValues.decode(listOf(mapOf("row_count" to 5L)), Tallies::class.java) as Tallies
    assertEquals(5L, tallies.single().rowCount)
  }

  @Test
  @DisplayName("Should match keys to the fields of the class that Jackson picks, and give a custom class the keys as written")
  fun shouldMatchKeysForTheClassThatJacksonPicks() {
    val custom = mapOf("kind" to "custom", "row_count" to 5L)

    assertEquals(5L, (ArgValues.decode(custom, ScheduledRun::class.java) as CustomRun).rowCount)
    assertEquals(5L, (ArgValues.decode(custom, Shift::class.java) as CustomShift).rowCount)
    val crew =
      ArgValues.decode(
        mapOf(
          "picked" to mapOf("row_count" to 5L),
          "typed" to custom,
          "typed_list" to listOf(custom),
          "content_picked" to listOf(mapOf("row_count" to 5L)),
        ),
        Crew::class.java,
      ) as Crew
    assertEquals(5L, crew.picked?.rowCount)
    assertEquals(5L, crew.typed?.rowCount)
    assertEquals(5L, crew.typedList?.single()?.rowCount)
    assertEquals(5L, crew.contentPicked?.single()?.rowCount)
    assertEquals(3L, (ArgValues.decode(mapOf("kind" to "daily", "row_count" to 3L), ScheduledRun::class.java) as DailyRun).rowCount)
    assertEquals(3L, (ArgValues.decode(mapOf("row_count" to 3L), LogBase::class.java) as PlainLog).rowCount)
  }

  @Test
  @DisplayName("Should match keys to a field whose type id is an external property, also under a field with JsonIgnoreProperties")
  fun shouldMatchKeysForExternalTypeIdField() {
    val keys = mapOf("payload_type" to "click", "event_payload" to mapOf("x_pos" to 3L))

    assertEquals(3, ((ArgValues.decode(keys, ClickEvent::class.java) as ClickEvent).eventPayload as ClickPayload).xPos)
    val holder = ArgValues.decode(mapOf("event" to keys), EventHolder::class.java) as EventHolder
    assertEquals(3, (holder.event?.eventPayload as ClickPayload).xPos)
  }

  @Test
  @DisplayName("Should keep a key with the name of an external type id as written, even when a field folds to that name")
  fun shouldKeepExternalTypeIdKeyAsWritten() {
    val keys = mapOf("payload_type" to "click", "payload" to mapOf("x_pos" to 3L))

    val event = ArgValues.decode(keys, ClickEventByIdName::class.java) as ClickEventByIdName

    assertEquals(3, (event.payload as ClickPayload).xPos)
    assertNull(event.payloadType)
  }

  @Test
  @DisplayName("Should match the keys of the elements of a list that as picks only the list class for")
  fun shouldMatchKeysInsideListClassThatAsPicks() {
    val history = ArgValues.decode(mapOf("history" to listOf(mapOf("row_count" to 1L))), History::class.java) as History

    assertEquals(1L, history.history?.single()?.rowCount)
  }

  @Test
  @DisplayName("Should take a key that equals a JsonAlias name as an exact match")
  fun shouldTakeAliasAsExactMatch() {
    val aliased = ArgValues.decode(mapOf("user_id" to "u1"), Aliased::class.java) as Aliased

    assertEquals("u1", aliased.owner)
    assertNull(aliased.userId)
    assertFailsWith(mapOf("tbl" to "a", "TARGET_TABLE" to "b"), AliasedTable::class.java, "Unrecognized field \"TARGET_TABLE\"")
  }

  @Test
  @DisplayName("Should match the keys of an object that Jackson merges into the value of a JsonMerge field")
  fun shouldMatchKeysOfMergedObject() {
    val rows = ArgValues.decode(mapOf("stats" to mapOf("row_count" to 5L)), MergedRows::class.java) as MergedRows

    assertEquals(5L, rows.stats.rowCount)
    assertEquals(0.5, rows.stats.threshold)
  }

  @Test
  @DisplayName("Should match the keys of the fields of a JsonUnwrapped field")
  fun shouldMatchKeysOfUnwrappedFields() {
    val rows = ArgValues.decode(mapOf("name" to "n", "row_count" to 5L), UnwrappedRows::class.java) as UnwrappedRows

    assertEquals(5L, rows.stats.rowCount)
  }

  @Test
  @DisplayName("Should fold keys onto the fields of a prefixed JsonUnwrapped field, but not onto a pinned one")
  fun shouldFoldOntoPrefixedUnwrappedFieldsExceptPinned() {
    val trip = ArgValues.decode(mapOf("SRC_CITY_NAME" to "Oslo", "src_ID" to "x"), Trip::class.java) as Trip
    assertEquals("Oslo", trip.src.cityName)
    assertEquals("x", trip.src.id)

    listOf("SRC_ID", "srcId", "src_id").forEach { key ->
      assertNull((ArgValues.decode(mapOf(key to "x"), Trip::class.java) as Trip).src.id) { key }
    }
  }

  @Test
  @DisplayName("Should reject a class whose field takes a key that folds like a key of a JsonUnwrapped field")
  fun shouldRejectFieldThatFoldsLikeKeyOfUnwrappedField() {
    val error = assertThrows(IllegalArgumentException::class.java) { ArgValues.decode(mapOf("customer_id" to "c1"), Order::class.java) }

    assertEquals(
      "Fields Order.customerId and Customer.id take the keys 'customerId' and 'customer_id', which differ only in " +
        "case or underscores, so the SDK cannot tell which of the two fields a key should go to. Rename one of the " +
        "fields.",
      error.message,
    )
  }

  @Test
  @DisplayName("Should give a key to the outer field when a JsonUnwrapped field takes the same key")
  fun shouldGiveSharedKeyToOuterField() {
    val rows = ArgValues.decode(mapOf("row_count" to 5L), ShadowedRows::class.java) as ShadowedRows

    assertEquals(5L, rows.rowCount)
    assertEquals(0L, rows.stats.rowCount)
  }

  @Test
  @DisplayName("Should match keys in a class that unwraps a field of its own class")
  fun shouldMatchKeysInClassThatUnwrapsItself() {
    val chain = ArgValues.decode(mapOf("NAME" to "a", "next_name" to "b"), Chain::class.java) as Chain

    assertEquals("a", chain.name)
    assertEquals("b", chain.next?.name)
  }

  @Test
  @DisplayName("Should check the keys of a class that unwraps a field of its own class")
  fun shouldCheckKeysOfClassThatUnwrapsItself() {
    val error =
      assertThrows(IllegalArgumentException::class.java) {
        ArgValues.decode(emptyMap<String, Any?>(), ChainWithNextName::class.java)
      }

    assertEquals(
      "Fields ChainWithNextName.nextName and ChainWithNextName.name take the keys 'nextName' and 'next_name', which " +
        "differ only in case or underscores, so the SDK cannot tell which of the two fields a key should go to. " +
        "Rename one of the fields.",
      error.message,
    )
  }

  @Test
  @DisplayName("Should not match a key to a JsonUnwrapped field, which takes no key of its own")
  fun shouldNotMatchKeyToUnwrappedField() {
    val value = ArgValues.decode(mapOf("STATS_COUNT" to 5L), UnwrappedStatsCount::class.java) as UnwrappedStatsCount

    assertEquals(5L, value.statscount)
  }

  @Test
  @DisplayName("Should count the key of a JsonUnwrapped field that Jackson cannot unwrap")
  fun shouldCountKeyOfFieldThatJacksonCannotUnwrap() {
    val error =
      assertThrows(IllegalArgumentException::class.java) {
        ArgValues.decode(emptyMap<String, Any?>(), UnwrappedMapClash::class.java)
      }

    assertEquals(
      "Fields UnwrappedMapClash.extra and UnwrappedMapClash.label take the keys 'extra' and 'Extra', which differ only " +
        "in case or underscores, so the SDK cannot tell which of the two fields a key should go to. Rename one of the " +
        "fields.",
      error.message,
    )
  }

  @Test
  @DisplayName("Should name the task argument when Jackson cannot read the class")
  fun shouldNameSourceWhenJacksonCannotReadClass() {
    assertFailsWith(mapOf("label" to "x"), DuplicateProperty::class.java, "Multiple fields representing property \"label\"")
    assertFailsWith(mapOf("window" to mapOf("start_time" to "x")), BrokenConverter::class.java, "has no default (no arg) constructor")
  }

  @Test
  @DisplayName("Should let JsonSetter skip or replace a null for a primitive field")
  fun shouldLetJsonSetterHandleNullForPrimitiveField() {
    val value = ArgValues.decode(mapOf("retries" to null, "limit" to null), SkipsNulls::class.java) as SkipsNulls

    assertEquals(3, value.retries)
    assertEquals(0, value.limit)
    assertEquals(3, (ArgValues.decode(mapOf("retries" to null), SetterSkipsNull::class.java) as SetterSkipsNull).retries)
    assertEquals(0, (ArgValues.decode(mapOf("retries" to null), CreatorAsEmpty::class.java) as CreatorAsEmpty).retries)
  }

  @Test
  @DisplayName("Should let the deserializer of a primitive field decide what a null reads as")
  fun shouldLetFieldDeserializerReadNull() {
    assertEquals(-1, (ArgValues.decode(mapOf("retries" to null), NullWithOwnValue::class.java) as NullWithOwnValue).retries)
  }

  @Test
  @DisplayName("Should leave the error to Jackson when JsonSetter makes a primitive field fail on null")
  fun shouldLeaveNullsFailToJackson() {
    assertFailsWith(mapOf("retries" to null), FailsOnNull::class.java, "Invalid `null` value encountered for property \"retries\"")
  }

  @Test
  @DisplayName("Should name the key and the parameter when a primitive constructor parameter that skips nulls gets null")
  fun shouldNameKeyForNullInPrimitiveConstructorParameter() {
    val error =
      assertThrows(IllegalArgumentException::class.java) {
        ArgValues.decode(mapOf("retries" to null), CreatorSkipsNull::class.java)
      }

    assertEquals(
      "Key 'retries' of a task argument is null, but field CreatorSkipsNull.retries has a primitive type that cannot " +
        "be null. Declare a boxed type (e.g. Integer instead of int) to receive null.",
      error.message,
    )
  }

  @Test
  @DisplayName("Should match the keys of nested objects ignoring case and underscores")
  fun shouldFoldKeysOfNestedObjects() {
    val report =
      ArgValues.decode(
        mapOf(
          "stats" to mapOf("row_count" to 1L),
          "HISTORY" to listOf(mapOf("ROW_COUNT" to 2L)),
          "by_region" to mapOf("Emea_Key" to mapOf("Row_Count" to 3L)),
        ),
        Report::class.java,
      ) as Report

    assertEquals(1L, report.stats?.rowCount)
    assertEquals(2L, report.history?.single()?.rowCount)
    assertEquals(setOf("Emea_Key"), report.byRegion?.keys)
    assertEquals(3L, report.byRegion?.get("Emea_Key")?.rowCount)
  }

  @Test
  @DisplayName("Should keep the element type of a generic type")
  fun shouldKeepElementTypeOfGenericType() {
    val type = object : TypeRef<List<Stats>>() {}.type

    @Suppress("UNCHECKED_CAST")
    val stats = ArgValues.decode(listOf(mapOf("row_count" to 1L)), type) as List<Stats>

    assertEquals(1L, stats.single().rowCount)
  }

  @Test
  @DisplayName("Should reject a class whose fields fold alike, counting pinned names")
  fun shouldRejectFieldsThatFoldAlike() {
    listOf(
      FoldClash::class.java to
        "Fields FoldClash.fooBar and FoldClash.foobar differ only in case or underscores, so the SDK cannot tell " +
        "which one a key should go to. Rename one of them.",
      PinnedFoldClash::class.java to
        "Fields PinnedFoldClash.pinned and PinnedFoldClash.fooBar take the keys 'foo_bar' and 'fooBar', which " +
        "differ only in case or underscores, so the SDK cannot tell which of the two fields a key should go to. " +
        "Rename one of the fields.",
    ).forEach { (type, expected) ->
      val error = assertThrows(IllegalArgumentException::class.java) { ArgValues.decode(emptyMap<String, Any?>(), type) }

      assertEquals(expected, error.message)
    }
  }

  @Test
  @DisplayName("Should name a task argument in the message about two keys that match one field, at any depth")
  fun shouldNameSourceForTwoKeysThatMatchOneField() {
    val keys = mapOf("row_count" to 1L, "ROW_COUNT" to 2L)
    listOf<Pair<Any, Type>>(
      keys to Stats::class.java,
      mapOf("stats" to keys) to Report::class.java,
      mapOf("history" to listOf(keys)) to Report::class.java,
      mapOf("by_region" to mapOf("emea" to keys)) to Report::class.java,
      mapOf("current" to keys) to Report::class.java,
    ).forEach { (value, type) ->
      val error = assertThrows(IllegalArgumentException::class.java) { ArgValues.decode(value, type) }

      assertEquals(
        "Keys 'row_count' and 'ROW_COUNT' of a task argument both match field Stats.rowCount when case and " +
          "underscores are ignored, so the SDK cannot tell which one to use. Name the field exactly as the key " +
          "it should take.",
        error.message,
      )
    }
  }

  @Test
  @DisplayName("Should name the key and the field when a task argument holds null for a primitive field")
  fun shouldNameKeyAndFieldForNullInPrimitiveField() {
    val error = assertThrows(IllegalArgumentException::class.java) { ArgValues.decode(mapOf("threshold" to null), Stats::class.java) }

    assertEquals(
      "Key 'threshold' of a task argument is null, but field Stats.threshold has a primitive type that cannot " +
        "be null. Declare a boxed type (e.g. Integer instead of int) to receive null.",
      error.message,
    )
  }

  @Test
  @DisplayName("Should reject what Jackson would otherwise turn into 0 for a primitive")
  fun shouldRejectValuesJacksonWouldTurnIntoZero() {
    assertFailsWith(mapOf("counts" to listOf(1L, null)), Stats::class.java, "[\"counts\"]")
    assertFailsWith(mapOf("row_count" to ""), Stats::class.java, "[\"rowCount\"]")
    assertFailsWith(mapOf("label" to "x"), CreatorStats::class.java, "\"rowCount\"")
  }

  @Test
  @DisplayName("Should leave null a constructor parameter that has no key and can hold null")
  fun shouldLeaveBoxedConstructorParameterNull() {
    assertNull((ArgValues.decode(mapOf("row_count" to 5L), CreatorStats::class.java) as CreatorStats).label)
  }

  @Test
  @DisplayName("Should fail on a number that the declared type cannot hold")
  fun shouldFailOnNumberTypeCannotHold() {
    listOf<Pair<Number, Type>>(
      3_000_000_000L to Int::class.javaObjectType,
      3_000_000_000L to Int::class.java,
      1e10 to Int::class.javaObjectType,
      BigInteger.valueOf(2).pow(64) to Long::class.javaObjectType,
      70_000L to Short::class.javaObjectType,
      300L to Byte::class.javaObjectType,
    ).forEach { (value, type) -> assertFailsWith(value, type, "out of range") }
  }

  @Test
  @DisplayName("Should convert a number that the declared type can hold, dropping a fraction for an integer type")
  fun shouldConvertNumberTypeCanHold() {
    listOf<Triple<Number, Type, Any>>(
      Triple(5L, Int::class.javaObjectType, 5),
      Triple(5L, Int::class.java, 5),
      Triple(5L, Short::class.javaObjectType, 5.toShort()),
      Triple(5L, Byte::class.javaObjectType, 5.toByte()),
      Triple(2L, Double::class.javaObjectType, 2.0),
      Triple(2L, Float::class.javaObjectType, 2f),
      Triple(1.5, Float::class.javaObjectType, 1.5f),
      Triple(5.9, Int::class.javaObjectType, 5),
      Triple(5.9, Long::class.javaObjectType, 5L),
      Triple(BigInteger.valueOf(5), Long::class.javaObjectType, 5L),
      Triple(5L, BigInteger::class.java, BigInteger.valueOf(5)),
      Triple(1.5, BigDecimal::class.java, BigDecimal("1.5")),
    ).forEach { (value, type, expected) -> assertEquals(expected, ArgValues.decode(value, type)) { "$value to $type" } }
  }

  @Test
  @DisplayName("Should return a value that already has the declared type as the same object")
  fun shouldReturnValueOfDeclaredTypeAsIs() {
    val value = mapOf("Key_A" to 1L)

    assertSame(value, ArgValues.decode(value, Map::class.java))
  }

  @Test
  @DisplayName("Should pass null through, even for a primitive type")
  fun shouldPassNullThrough() {
    assertNull(ArgValues.decode(null, Int::class.java))
  }
}
