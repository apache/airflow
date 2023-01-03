# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Collection, Iterable, Sequence

from airflow.utils.context import Context
from airflow.utils.helpers import render_template_as_native, render_template_to_string
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.mixins import ResolveMixin
from airflow.utils.session import NEW_SESSION, provide_session

if TYPE_CHECKING:
    import jinja2
    from sqlalchemy.orm import Session

    from airflow import DAG


class Templater(LoggingMixin):
    """
    This renders the template fields of object.

    :meta private:
    """

    # For derived classes to define which fields will get jinjaified.
    template_fields: Collection[str]
    # Defines which files extensions to look for in the templated fields.
    template_ext: Sequence[str]

    def get_template_env(self, dag: DAG | None = None) -> jinja2.Environment:
        """Fetch a Jinja template environment from the DAG or instantiate empty environment if no DAG."""
        # This is imported locally since Jinja2 is heavy and we don't need it
        # for most of the functionalities. It is imported by get_template_env()
        # though, so we don't need to put this after the 'if dag' check.
        from airflow.templates import SandboxedEnvironment

        if dag:
            return dag.get_template_env(force_sandboxed=False)
        return SandboxedEnvironment(cache_size=0)

    def prepare_template(self) -> None:
        """Hook triggered after the templated fields get replaced by their content.

        If you need your object to alter the content of the file before the
        template is rendered, it should override this method to do so.
        """

    def resolve_template_files(self) -> None:
        """Getting the content of files for template_field / template_ext."""
        if self.template_ext:
            for field in self.template_fields:
                content = getattr(self, field, None)
                if content is None:
                    continue
                elif isinstance(content, str) and any(content.endswith(ext) for ext in self.template_ext):
                    env = self.get_template_env()
                    try:
                        setattr(self, field, env.loader.get_source(env, content)[0])  # type: ignore
                    except Exception:
                        self.log.exception("Failed to resolve template field %r", field)
                elif isinstance(content, list):
                    env = self.get_template_env()
                    for i, item in enumerate(content):
                        if isinstance(item, str) and any(item.endswith(ext) for ext in self.template_ext):
                            try:
                                content[i] = env.loader.get_source(env, item)[0]  # type: ignore
                            except Exception:
                                self.log.exception("Failed to get source %s", item)
        self.prepare_template()

    @provide_session
    def _do_render_template_fields(
        self,
        parent: Any,
        template_fields: Iterable[str],
        context: Context,
        jinja_env: jinja2.Environment,
        seen_oids: set[int],
        *,
        session: Session = NEW_SESSION,
    ) -> None:
        for attr_name in template_fields:
            value = getattr(parent, attr_name)
            rendered_content = self.render_template(
                value,
                context,
                jinja_env,
                seen_oids,
            )
            if rendered_content:
                setattr(parent, attr_name, rendered_content)

    def _render(self, template, context, dag: DAG | None = None) -> Any:
        if dag and dag.render_template_as_native_obj:
            return render_template_as_native(template, context)
        return render_template_to_string(template, context)

    def render_template(
        self,
        content: Any,
        context: Context,
        jinja_env: jinja2.Environment | None = None,
        seen_oids: set[int] | None = None,
    ) -> Any:
        """Render a templated string.

        If *content* is a collection holding multiple templated strings, strings
        in the collection will be templated recursively.

        :param content: Content to template. Only strings can be templated (may
            be inside a collection).
        :param context: Dict with values to apply on templated content
        :param jinja_env: Jinja environment. Can be provided to avoid
            re-creating Jinja environments during recursion.
        :param seen_oids: template fields already rendered (to avoid
            *RecursionError* on circular dependencies)
        :return: Templated content
        """
        # "content" is a bad name, but we're stuck to it being public API.
        value = content
        del content

        if seen_oids is not None:
            oids = seen_oids
        else:
            oids = set()

        if id(value) in oids:
            return value

        if not jinja_env:
            jinja_env = self.get_template_env()

        if isinstance(value, str):
            if any(value.endswith(ext) for ext in self.template_ext):  # A filepath.
                template = jinja_env.get_template(value)
            else:
                template = jinja_env.from_string(value)
            return self._render(template, context)
        if isinstance(value, ResolveMixin):
            return value.resolve(context)

        # Fast path for common built-in collections.
        if value.__class__ is tuple:
            return tuple(self.render_template(element, context, jinja_env, oids) for element in value)
        elif isinstance(value, tuple):  # Special case for named tuples.
            return value.__class__(*(self.render_template(el, context, jinja_env, oids) for el in value))
        elif isinstance(value, list):
            return [self.render_template(element, context, jinja_env, oids) for element in value]
        elif isinstance(value, dict):
            return {k: self.render_template(v, context, jinja_env, oids) for k, v in value.items()}
        elif isinstance(value, set):
            return {self.render_template(element, context, jinja_env, oids) for element in value}

        # More complex collections.
        self._render_nested_template_fields(value, context, jinja_env, oids)
        return value

    def _render_nested_template_fields(
        self,
        value: Any,
        context: Context,
        jinja_env: jinja2.Environment,
        seen_oids: set[int],
    ) -> None:
        if id(value) in seen_oids:
            return
        seen_oids.add(id(value))
        try:
            nested_template_fields = value.template_fields
        except AttributeError:
            # content has no inner template fields
            return
        self._do_render_template_fields(value, nested_template_fields, context, jinja_env, seen_oids)
