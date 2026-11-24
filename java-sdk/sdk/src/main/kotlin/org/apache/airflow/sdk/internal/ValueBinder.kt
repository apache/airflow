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

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.core.JsonToken
import com.fasterxml.jackson.databind.BeanDescription
import com.fasterxml.jackson.databind.DatabindException
import com.fasterxml.jackson.databind.DeserializationConfig
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.JsonDeserializer
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.PropertyName
import com.fasterxml.jackson.databind.deser.BeanDeserializerBase
import com.fasterxml.jackson.databind.deser.BeanDeserializerModifier
import com.fasterxml.jackson.databind.deser.CreatorProperty
import com.fasterxml.jackson.databind.deser.SettableBeanProperty
import com.fasterxml.jackson.databind.deser.impl.NullsConstantProvider
import com.fasterxml.jackson.databind.deser.impl.NullsFailProvider
import com.fasterxml.jackson.databind.deser.std.DelegatingDeserializer
import com.fasterxml.jackson.databind.introspect.Annotated
import com.fasterxml.jackson.databind.introspect.AnnotatedField
import com.fasterxml.jackson.databind.introspect.JacksonAnnotationIntrospector
import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.databind.util.NameTransformer
import org.apache.airflow.sdk.ArgName
import java.lang.reflect.Modifier
import java.lang.reflect.Type

// A task declares the values that it reads from its arguments and from XComs,
// so a key that the target class does not declare fails the task. The Go SDK
// also fails a task when an argument value has a key that the target struct
// does not declare.
private val strictBinder = ValueBinder(failOnUnknownKeys = true)

// Other tasks in the Dag run may read keys that the target class does not
// declare, so those keys are ignored here.
private val confBinder = ValueBinder(failOnUnknownKeys = false)

internal fun bindArgument(
  value: Any?,
  type: Type,
): Any? = strictBinder.bind(value, type, "a task argument")

internal fun bindXCom(
  value: Any?,
  type: Type,
): Any? = strictBinder.bind(value, type, "the XCom value")

@Suppress("UNCHECKED_CAST")
internal fun <T : Any> bindConf(
  conf: Map<String, Any?>,
  type: Type,
): T = confBinder.convert(conf, type, "the Dag run conf") as T

internal fun writeConfJson(conf: Map<String, Any?>): String = confBinder.writeJson(conf)

/**
 * Makes Jackson use the name in [ArgName] as the property name of the annotated
 * field. That field then reads from and writes to the key with that name.
 */
internal object ArgNameIntrospector : JacksonAnnotationIntrospector() {
  override fun findNameForDeserialization(a: Annotated): PropertyName? = findPinnedName(a) ?: super.findNameForDeserialization(a)

  override fun findNameForSerialization(a: Annotated): PropertyName? = findPinnedName(a) ?: super.findNameForSerialization(a)

  private fun findPinnedName(a: Annotated): PropertyName? = a.getAnnotation(ArgName::class.java)?.let { PropertyName.construct(it.value) }
}

/**
 * Converts values from the wire into the types a task declares.
 * [org.apache.airflow.sdk.DagRun.getConf] documents how keys match fields.
 */
private class ValueBinder(
  failOnUnknownKeys: Boolean,
) {
  // By default, Jackson moves a date-time that carries an offset to UTC. For
  // example, 2024-01-31T02:00+08:00 would become 2024-01-30T18:00Z and fall on
  // another date. This mapper keeps the offset as written instead.
  //
  // FAIL_ON_NULL_FOR_PRIMITIVES stops Jackson from turning a null into 0, for
  // example in an int[]. It also rejects a record whose primitive component has
  // no key, because Jackson handles the missing value as null.
  private val mapper: ObjectMapper by lazy {
    JsonMapper
      .builder()
      .annotationIntrospector(ArgNameIntrospector)
      .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, failOnUnknownKeys)
      .disable(DeserializationFeature.ADJUST_DATES_TO_CONTEXT_TIME_ZONE)
      .enable(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES)
      .addModule(KeyMatching)
      .build()
      .findAndRegisterModules()
  }

  /**
   * Converts [value] into [type], passing null through. A value that already
   * has the type stays the same object, so reading a large XCom into a raw `Map`
   * does not copy it.
   *
   * @param source What [value] is, for the error messages.
   */
  fun bind(
    value: Any?,
    type: Type,
    source: String,
  ): Any? {
    if (value == null) return null
    if (type is Class<*> && type.isInstance(value)) return value
    return convert(value, type, source)
  }

  /**
   * Converts [value] into a new object of [type], even if [value] already has
   * that type.
   *
   * @param source What [value] is, for the error messages.
   */
  fun convert(
    value: Any,
    type: Type,
    source: String,
  ): Any? =
    try {
      mapper.convertValue<Any?>(value, mapper.constructType(type))
    } catch (e: RuntimeException) {
      val mismatch = generateSequence<Throwable>(e) { it.cause }.filterIsInstance<KeyMatchException>().firstOrNull()
      when {
        mismatch != null -> throw IllegalArgumentException(mismatch.describe(source), e)
        e is IllegalArgumentException -> throw IllegalArgumentException("Cannot convert $source to ${type.typeName}: ${e.message}", e)
        else -> throw e
      }
    }

  fun writeJson(value: Any?): String = mapper.writeValueAsString(value)
}

/**
 * Matches keys to field names inside Jackson. It wraps every deserializer that
 * reads an object field by field, so a deserializer, a converter or a
 * delegating creator that reads an object itself gets the keys as written.
 */
private object KeyMatching : SimpleModule("org.apache.airflow.sdk.KeyMatching") {
  init {
    setDeserializerModifier(
      object : BeanDeserializerModifier() {
        override fun modifyDeserializer(
          config: DeserializationConfig,
          beanDesc: BeanDescription,
          deserializer: JsonDeserializer<*>,
        ): JsonDeserializer<*> =
          if (deserializer is BeanDeserializerBase && !deserializer.valueInstantiator.canCreateUsingDelegate()) {
            KeyMatchingDeserializer(deserializer, DeclaredProperties(deserializer))
          } else {
            deserializer
          }
      },
    )
  }
}

/**
 * Renames the keys of an object to the names of the properties that they match,
 * right before the wrapped bean deserializer reads the object.
 *
 * @property declared The properties of the class, recorded before Jackson
 *    resolved the bean deserializer. Every copy of this
 *    `KeyMatchingDeserializer` that Jackson makes shares the same recorded
 *    properties.
 */
private class KeyMatchingDeserializer(
  delegatee: JsonDeserializer<*>,
  val declared: DeclaredProperties,
) : DelegatingDeserializer(delegatee) {
  @Volatile
  private var cachedKeys: PropertyKeys? = null

  override fun newDelegatingInstance(newDelegatee: JsonDeserializer<*>): JsonDeserializer<*> =
    KeyMatchingDeserializer(newDelegatee, declared)

  override fun deserialize(
    p: JsonParser,
    ctxt: DeserializationContext,
  ): Any? = _delegatee.deserialize(matchKeys(p, ctxt) ?: p, ctxt)

  @Suppress("UNCHECKED_CAST")
  override fun deserialize(
    p: JsonParser,
    ctxt: DeserializationContext,
    intoValue: Any?,
  ): Any? = (_delegatee as JsonDeserializer<Any?>).deserialize(matchKeys(p, ctxt) ?: p, ctxt, intoValue)

  /**
   * Returns a parser over the object at [p] with its keys renamed, or null when
   * [p] is not at an object. The wrapped bean deserializer reads the object
   * from the returned parser. This function leaves [p] at the end of the
   * object, where the bean deserializer would leave it.
   */
  private fun matchKeys(
    p: JsonParser,
    ctxt: DeserializationContext,
  ): JsonParser? {
    val bean = _delegatee as? BeanDeserializerBase ?: return null
    var token = p.currentToken()
    if (token != JsonToken.START_OBJECT && token != JsonToken.FIELD_NAME) return null
    val asWritten = ctxt.bufferForInputBuffering(p)
    val keys = mutableListOf<String>()
    val nullKeys = mutableSetOf<String>()
    if (token == JsonToken.START_OBJECT) token = p.nextToken()
    asWritten.writeStartObject()
    while (token == JsonToken.FIELD_NAME) {
      val key = p.currentName()
      keys += key
      asWritten.writeFieldName(key)
      if (p.nextToken() == JsonToken.VALUE_NULL) nullKeys += key
      asWritten.copyCurrentStructure(p)
      token = p.nextToken()
    }
    asWritten.writeEndObject()
    val renames = propertyKeys(bean, ctxt).match(keys, nullKeys, ctxt)
    if (renames.isEmpty()) return asWritten.asParserOnFirstToken()
    val renamed = ctxt.bufferForInputBuffering(p)
    val copy = asWritten.asParserOnFirstToken()
    renamed.writeStartObject()
    while (copy.nextToken() == JsonToken.FIELD_NAME) {
      val key = copy.currentName()
      renamed.writeFieldName(renames[key] ?: key)
      copy.nextToken()
      renamed.copyCurrentStructure(copy)
    }
    renamed.writeEndObject()
    return renamed.asParserOnFirstToken()
  }

  private fun propertyKeys(
    bean: BeanDeserializerBase,
    ctxt: DeserializationContext,
  ): PropertyKeys = cachedKeys ?: PropertyKeys.of(bean, declared, ctxt).also { cachedKeys = it }
}

/**
 * This object copies the properties that Jackson built for one class, before
 * Jackson resolves the bean deserializer. Resolving removes two kinds of
 * property from [BeanDeserializerBase.properties]. The first kind is a
 * `@JsonUnwrapped` property whose type Jackson can unwrap. Jackson can unwrap a
 * class with fields, but Jackson cannot unwrap a `Map`. The second kind is a
 * property with an EXTERNAL_PROPERTY type id. After resolving, the deserializer
 * no longer lists properties of either kind.
 */
private class DeclaredProperties(
  private val bean: BeanDeserializerBase,
) {
  private val all = bean.properties().asSequence().toList()

  /**
   * Finds the properties with an EXTERNAL_PROPERTY type id. Call this function
   * only after Jackson has resolved the bean deserializer.
   */
  fun findExternal(ctxt: DeserializationContext): List<SettableBeanProperty> {
    val kept = findKeptNames()
    return all.filter { it.name !in kept && findUnwrapper(it, ctxt) == null }
  }

  /**
   * Checks that no two keys that the class takes differ only in case or
   * underscores. The keys include the keys of each class that a
   * `@JsonUnwrapped` field reads, with the prefix and the suffix from
   * `@JsonUnwrapped`. The check walks into each of those classes and into the
   * unwrapped classes inside them. When a class unwraps a field of its own
   * class, directly or through other classes, the walk reaches that class a
   * second time. The walk then adds the keys of that class's own fields once
   * more, with the prefix and the suffix, and stops there. Two equal keys pass
   * the check. Jackson gives a shared key to an outer field that has the key as
   * its name. If no outer field has that name, Jackson gives the key to every
   * `@JsonUnwrapped` field that takes the key.
   */
  fun checkKeyNames(ctxt: DeserializationContext) {
    val byFold = mutableMapOf<String, KeyName>()
    findKeyNames(ctxt, emptySet()).forEach { name ->
      val previous = byFold.putIfAbsent(foldArgName(name.key), name)
      if (previous != null && previous.key != name.key) {
        throw KeyMatchException { previous.describeClash(name) }
      }
    }
  }

  private fun findKeyNames(
    ctxt: DeserializationContext,
    visited: Set<DeclaredProperties>,
  ): List<KeyName> {
    val expand = this !in visited
    val owner = bean.handledType().simpleName
    val kept = findKeptNames()
    return all.flatMap { property ->
      val unwrapper = if (property.name in kept) null else findUnwrapper(property, ctxt)
      when {
        unwrapper == null -> listOf(KeyName(property.name, owner, (property.member as? AnnotatedField)?.name ?: property.name))
        !expand -> emptyList()
        else -> {
          val unwrapped = ctxt.findNonContextualValueDeserializer(property.type) as? KeyMatchingDeserializer
          val names = unwrapped?.declared?.findKeyNames(ctxt, visited + this).orEmpty()
          names.map { it.copy(key = unwrapper.transform(it.key)) }
        }
      }
    }
  }

  private fun findKeptNames(): Set<String> = bean.properties().asSequence().mapTo(mutableSetOf()) { it.name }

  private fun findUnwrapper(
    property: SettableBeanProperty,
    ctxt: DeserializationContext,
  ): NameTransformer? = property.member?.let { ctxt.annotationIntrospector.findUnwrappingNameTransformer(it) }
}

/**
 * A key that a class takes, with the field that the key sets.
 *
 * @property key The key. For a field of a class that a `@JsonUnwrapped` field
 *    reads, the key includes the prefix and the suffix from `@JsonUnwrapped`.
 * @property owner The simple name of the class that Jackson reads. The class
 *    either declares the field or inherits the field from a superclass.
 * @property field The Java name of the field, when Jackson sets the value
 *    through a field. When Jackson sets the value through a setter or a
 *    constructor parameter, the name of the property is used instead. The name
 *    of the property is the key without the prefix or the suffix from
 *    `@JsonUnwrapped`.
 */
private data class KeyName(
  val key: String,
  val owner: String,
  val field: String,
) {
  fun describeClash(other: KeyName): String =
    if (key == field && other.key == other.field) {
      "Fields $owner.$field and ${other.owner}.${other.field} differ only in case or underscores, so the SDK " +
        "cannot tell which one a key should go to. Rename one of them."
    } else {
      "Fields $owner.$field and ${other.owner}.${other.field} take the keys '$key' and '${other.key}', which differ " +
        "only in case or underscores, so the SDK cannot tell which of the two fields a key should go to. Rename one " +
        "of the fields."
    }
}

/**
 * The names that the keys of an object can match for one bean deserializer.
 *
 * @property exact Each property name and each `@JsonAlias` name, with its
 *    property. A key with one of these names is used as written.
 * @property byFold Each property, keyed by its folded token from [foldArgName].
 *    A property whose name [ArgName] pins is left out.
 * @property typeIdNames The names of the external type ids of the class.
 */
private class PropertyKeys(
  private val className: String,
  private val exact: Map<String, SettableBeanProperty>,
  private val byFold: Map<String, SettableBeanProperty>,
  private val typeIdNames: Set<String>,
) {
  /**
   * Returns the new name of each key that matches a property only when case and
   * underscores are ignored. Such a key loses to another key that has an exact
   * name of the same property. The losing key stays as written, so Jackson
   * ignores it or fails on it. A key that equals the name of an external type
   * id also stays as written, so that Jackson reads the key as the type id.
   */
  fun match(
    keys: List<String>,
    nullKeys: Set<String>,
    ctxt: DeserializationContext,
  ): Map<String, String> {
    val present = keys.toSet()
    val keyOfProperty = mutableMapOf<String, String>()
    val renames = mutableMapOf<String, String>()
    keys.forEach { key ->
      if (key !in exact && key in typeIdNames) return@forEach
      val property =
        exact[key] ?: byFold[foldArgName(key)]?.takeUnless { candidate ->
          exact.any { (name, property) -> property === candidate && name in present }
        } ?: return@forEach
      if (key !in exact) {
        val other = keyOfProperty.put(property.name, key)
        if (other != null) {
          throw KeyMatchException { source ->
            "Keys '$other' and '$key' of $source both match field $className.${property.name} when case and " +
              "underscores are ignored, so the SDK cannot tell which one to use. Name the field exactly as the key " +
              "it should take."
          }
        }
        renames[key] = property.name
      }
      if (key in nullKeys && property.failsOnNull(ctxt)) {
        throw KeyMatchException { source ->
          "Key '$key' of $source is null, but field $className.${property.name} has a primitive type that cannot " +
            "be null. Declare a boxed type (e.g. Integer instead of int) to receive null."
        }
      }
    }
    return renames
  }

  companion object {
    fun of(
      bean: BeanDeserializerBase,
      declared: DeclaredProperties,
      ctxt: DeserializationContext,
    ): PropertyKeys {
      checkPinnedNames(bean.handledType())
      declared.checkKeyNames(ctxt)
      val external = declared.findExternal(ctxt)
      val exact = mutableMapOf<String, SettableBeanProperty>()
      val byFold = mutableMapOf<String, SettableBeanProperty>()
      (bean.properties().asSequence() + external).forEach { property ->
        exact[property.name] = property
        property.findAliases(ctxt.config).forEach { alias -> exact.putIfAbsent(alias.simpleName, property) }
        if (property.member?.hasAnnotation(ArgName::class.java) != true) byFold[foldArgName(property.name)] = property
      }
      val typeIdNames = external.mapNotNullTo(mutableSetOf()) { it.valueTypeDeserializer?.propertyName }
      return PropertyKeys(bean.handledType().simpleName, exact, byFold, typeIdNames)
    }
  }
}

/**
 * Returns whether Jackson fails when this property gets a null because the
 * property has a primitive type. `@JsonSetter(nulls = Nulls.FAIL)` makes
 * Jackson fail a null for any type, so this function returns false for a
 * property with `Nulls.FAIL` and leaves the error to Jackson.
 * `@JsonSetter(nulls = Nulls.SKIP)` makes Jackson skip a null for a field or a
 * setter, but Jackson still passes the null to a constructor parameter.
 * Otherwise, the null provider of the property decides. The null provider also
 * accounts for `Nulls.AS_EMPTY` and for a deserializer with its own null value.
 */
private fun SettableBeanProperty.failsOnNull(ctxt: DeserializationContext): Boolean {
  if (!type.isPrimitive) return false
  val provider = nullValueProvider
  if (provider is NullsFailProvider) return false
  if (NullsConstantProvider.isSkipper(provider)) return this is CreatorProperty
  return try {
    provider.getNullValue(ctxt) == null
  } catch (e: DatabindException) {
    true
  }
}

/**
 * Checks that no other field of [type] or of the superclasses of [type] has the
 * name that [ArgName] pins on a field.
 *
 * @throws KeyMatchException if another field claims the same name as a pinned
 *    field. Without this check, Jackson can merge the two fields into one
 *    property and drop one of the two values without an error. This function
 *    reads the Java fields directly, not the properties that Jackson builds,
 *    because Jackson has already merged the two fields in those properties.
 */
private fun checkPinnedNames(type: Class<*>) {
  val fields =
    generateSequence(type) { it.superclass }
      .flatMap { it.declaredFields.asSequence() }
      .filterNot { Modifier.isStatic(it.modifiers) }
      .toList()
  fields.filter(::isPinned).forEach { field ->
    val name = argNameOf(field)
    val other = fields.find { it != field && argNameOf(it) == name }
    if (other != null) {
      throw KeyMatchException {
        "Fields ${field.declaringClass.simpleName}.${field.name} and ${other.declaringClass.simpleName}.${other.name} " +
          "both claim the name '$name', so the SDK cannot tell which one a key should go to. Rename one of them."
      }
    }
  }
}

/**
 * Reports a key or a class that [PropertyKeys] cannot match. Jackson wraps this
 * exception when it is thrown in a nested object. So [ValueBinder.convert]
 * looks for it among the causes of the exception that Jackson throws, and
 * builds the message with the source of the value.
 */
private class KeyMatchException(
  val describe: (source: String) -> String,
) : RuntimeException() {
  override val message: String
    get() = describe("the value")
}
