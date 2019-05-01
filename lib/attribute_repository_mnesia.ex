defmodule AttributeRepositoryMnesia do
  @moduledoc """
  Documentation for AttributeRepositoryMnesia.
  """

  require Logger

  use AttributeRepository.Read
  use AttributeRepository.Write
  use AttributeRepository.Search

  alias AttributeRepository.Search.AttributePath

  @behaviour AttributeRepository.Start
  @behaviour AttributeRepository.Install
  @behaviour AttributeRepository.Read
  @behaviour AttributeRepository.Write
  @behaviour AttributeRepository.Search

  @impl AttributeRepository.Start

  def start(_init_opts) do
    :mnesia.start()
  end

  @impl AttributeRepository.Install

  def install(run_opts, init_opts) do
    :mnesia.stop()

    :mnesia.create_schema([node()])

    :mnesia.start()

    case :mnesia.create_table(run_opts[:instance], [
      attributes: [:id, :attribute, :value],
      type: :bag,
      index: [:value]
    ] ++ (init_opts[:mnesia_config] || [])) do
      {:atomic, :ok} ->
        Logger.debug("#{__MODULE__}: created table of instance #{run_opts[:instance]}")

        :ok

      {:aborted, {:already_exists, _}} ->
        :ok

      {:aborted, reason} ->
        Logger.error("#{__MODULE__}: failed to create table of instance " <>
          "#{run_opts[:instance]} (reason: #{inspect reason})")

        {:error, reason}
    end
  end

  @impl AttributeRepository.Read

  def get(resource_id, :all, run_opts) do
    case :mnesia.transaction(fn -> :mnesia.read(run_opts[:instance], resource_id) end) do
      {:atomic, [_element | _] = record_list} ->
        {:ok, build_result(record_list)}

      {:atomic, []} ->
        {:error, AttributeRepository.Read.NotFoundError.exception([])}

      _ ->
        {:error, AttributeRepository.ReadError.exception([])}
    end
  end

  #FIXME: what if none attributes are present but the entity does exist?
  #it now returns NotFoundError exception but should not

  def get(resource_id, attribute_list, run_opts) do
    case :mnesia.transaction(fn ->
      match_spec = Enum.reduce(
        attribute_list,
        [],
        fn
          attribute, acc ->
            [{{run_opts[:instance], resource_id, attribute, :"$1"},
              [],
              [{{attribute, :"$1"}}]}]
            ++ acc
        end
      )

      :mnesia.select(run_opts[:instance], match_spec)
    end) do
      {:atomic, [_element | _] = record_list} ->
        {:ok, build_result(record_list)}

      {:atomic, []} ->
        {:error, AttributeRepository.Read.NotFoundError.exception([])}

      _ ->
        {:error, AttributeRepository.ReadError.exception([])}
    end
  end

  defp build_result(record_list) do
    Enum.reduce(
      record_list,
      %{},
      fn
        {_table, _resource_id, attribute, value}, res ->
          value =
            case value do
              {:datetime, dt_value} ->
                elem(DateTime.from_unix(dt_value), 1)

              %{} ->
                Enum.reduce(
                  value,
                  %{},
                  fn
                    {key, {:datetime, dt_value}}, acc ->
                      Map.put(acc, key, elem(DateTime.from_unix(dt_value), 1))

                    {key, value}, acc ->
                      Map.put(acc, key, value)
                  end
                )

              _ ->
                value
            end

          case Map.get(res, attribute) do
            nil ->
              Map.put(res, attribute, value)

            l when is_list(l) ->
              Map.put(res, attribute, [value | l])

            simple_val ->
              Map.put(res, attribute, [value, simple_val])
          end
      end
    )
  end

  @impl AttributeRepository.Write

  def put(resource_id, %{} = resource, run_opts) do
    case :mnesia.transaction(fn ->
      # first we destroy the whole record
      :mnesia.delete({run_opts[:instance], resource_id})
      for {attr, val} <- resource do
        case val do
          [_ | _] ->
            Enum.each(val, fn elmt -> put_attribute(resource_id, attr, elmt, run_opts) end)

          _ ->
            put_attribute(resource_id, attr, val, run_opts)
        end
      end
    end) do
      {:atomic, _} ->
        Logger.debug("#{__MODULE__}: written `#{inspect resource}` " <>
          "for resource_id `#{inspect resource_id}` " <>
          "of instance #{run_opts[:instance]}")

        {:ok, resource}

      {:aborted, reason} ->
        IO.inspect(reason)
        Logger.error("#{__MODULE__}: failed to write #{inspect resource} " <>
          "for resource_id `#{inspect(resource_id)}` " <>
          "of instance #{run_opts[:instance]} (reason: #{inspect reason})")

        {:error, AttributeRepository.WriteError.exception([])}
    end
  end

  defp put_attribute(resource_id, attr, val, run_opts) do
    :mnesia.write({
      run_opts[:instance],
      resource_id,
      attr,
      transform_value_before_insert(val)
    })
  end

  defp transform_value_before_insert(%DateTime{} = datetime) do
    {:datetime, DateTime.to_unix(datetime)}
  end

  defp transform_value_before_insert(%{} = map) do
    Enum.reduce(
      map,
      %{},
      fn
        {key, %DateTime{} = value}, acc ->
          Map.put(acc, key, {:datetime, DateTime.to_unix(value)})

        {key, value}, acc ->
          Map.put(acc, key, value)
      end
    )
  end

  defp transform_value_before_insert(value), do: value

  @impl AttributeRepository.Write

  def modify(resource_id, op_list, run_opts) do
    case :mnesia.transaction(fn ->
      Enum.each(
        op_list,
        fn op -> modify_op(resource_id, op, run_opts) end
      )
    end) do
      {:atomic, _} ->
        Logger.debug("#{__MODULE__}: modified with `#{inspect op_list}` " <>
          "for resource_id `#{inspect resource_id}` " <>
          "of instance #{run_opts[:instance]}")

        :ok

      {:aborted, reason} ->
        IO.inspect(reason)
        Logger.error("#{__MODULE__}: failed to modify with #{inspect op_list} " <>
          "for resource_id `#{inspect(resource_id)}` " <>
          "of instance #{run_opts[:instance]} (reason: #{inspect reason})")

        {:error, AttributeRepository.WriteError.exception([])}
    end
  end

  defp modify_op(resource_id, {:add, attribute, value}, run_opts) do
    :mnesia.write({
      run_opts[:instance],
      resource_id,
      attribute,
      transform_value_before_insert(value)
    })
  end

  defp modify_op(resource_id, {:replace, attribute, value}, run_opts) do

    Enum.each(
      :mnesia.match_object({run_opts[:instance], resource_id, attribute, :"_"}),
      fn object -> :mnesia.delete_object(object) end
    )

    :mnesia.write({
      run_opts[:instance],
      resource_id,
      attribute,
      transform_value_before_insert(value)
    })
  end

  defp modify_op(resource_id, {:replace, attribute, old_value, new_value}, run_opts) do
    :mnesia.delete_object({run_opts[:instance], resource_id, attribute, old_value})

    :mnesia.write({
      run_opts[:instance],
      resource_id,
      attribute,
      transform_value_before_insert(new_value)
    })
  end

  defp modify_op(resource_id, {:delete, attribute}, run_opts) do
    Enum.each(
      :mnesia.match_object({run_opts[:instance], resource_id, attribute, :"_"}),
      fn object -> :mnesia.delete_object(object) end
    )
  end

  defp modify_op(resource_id, {:delete, attribute, value}, run_opts) do
    :mnesia.delete_object({run_opts[:instance], resource_id, attribute, value})
  end

  @impl AttributeRepository.Write

  def delete(resource_id, run_opts) do
    case :mnesia.transaction(fn ->
      :mnesia.delete({run_opts[:instance], resource_id})
    end) do
      {:atomic, _} ->
        Logger.debug("#{__MODULE__}: deleted resource_id `#{inspect resource_id}` " <>
          "of instance #{run_opts[:instance]}")

        :ok

      {:aborted, reason} ->
        Logger.error("#{__MODULE__}: failed to delete resource_id `#{inspect resource_id}` " <>
          "of instance #{run_opts[:instance]} (reason: #{inspect reason})")

        {:error, AttributeRepository.WriteError.exception([])}
    end
  end

  @impl AttributeRepository.Search

  def search(filter, attributes, run_opts) do
    for resource_id <- Enum.dedup(do_search(filter, run_opts)) do
      {resource_id, get!(resource_id, attributes, run_opts)}
    end
  rescue
    e ->
      {:error, e}
  end

  @spec do_search(any(), AttributeRepository.run_opts()) :: [AttributeRepository.resource_id]

  defp do_search({:attrExp, attrExp}, run_opts) do
    do_search(attrExp, run_opts)
  end

  defp do_search({:and, lhs, rhs}, run_opts) do
    l_res = MapSet.new(do_search(lhs, run_opts))

    if MapSet.size(l_res) == 0 do
      []
    else
      l_res
      |> MapSet.intersection(MapSet.new(do_search(rhs, run_opts)))
      |> MapSet.to_list()
    end
  end

  defp do_search({:or, lhs, rhs}, run_opts) do
    l_res = MapSet.new(do_search(lhs, run_opts))
    r_res = MapSet.new(do_search(rhs, run_opts))

    MapSet.union(l_res, r_res)
    |> MapSet.to_list()
  end

  defp do_search({:not, filter}, run_opts) do
    all_keys = MapSet.new(:mnesia.dirty_all_keys(run_opts[:instance]))

    search_result = MapSet.new(do_search(filter, run_opts))

    MapSet.difference(all_keys, search_result)
    |> MapSet.to_list()

    #FIXME: probably very inneficient
    # alternative below but it's hard to handle the :pr operator negation with mnesia bag tables

    #filter
    #|> negate_expression()
    #|> do_search(run_opts)
  end

  defp do_search({op, %AttributePath{attribute: attribute} = attr_path, value},
                 run_opts)
  when is_binary(value) do
    match_spec =
      [
        {
          {
            run_opts[:instance],
            :"$1",
            attribute,
            attribute_path_to_match_spec_field(attr_path)
          },
          [{:is_binary, :"$2"}, {op_to_match_spec_atom_op(op), :'$2', value}],
          [:"$1"]
        }
      ]

    :mnesia.dirty_select(run_opts[:instance], match_spec)
  end

  defp do_search({op, %AttributePath{attribute: attribute} = attr_path, value},
                 run_opts)
  when is_boolean(value) and op in [:eq, :ne] do
    match_spec =
      [
        {
          {
            run_opts[:instance],
            :"$1",
            attribute,
            attribute_path_to_match_spec_field(attr_path)
          },
          [{op_to_match_spec_atom_op(op), :'$2', value}],
          [:"$1"]
        }
      ]

    :mnesia.dirty_select(run_opts[:instance], match_spec)
  end

  defp do_search({_op, %AttributePath{}, value}, _) when is_boolean(value) do
    raise "Invalid operation for boolean"
  end


  defp do_search({op, %AttributePath{attribute: attribute} = attr_path, value},
                 run_opts)
  when is_float(value) do
    match_spec =
      [
        {
          {
            run_opts[:instance],
            :"$1",
            attribute,
            attribute_path_to_match_spec_field(attr_path)
          },
          [{:is_float, :"$2"}, {op_to_match_spec_atom_op(op), :'$2', value}],
          [:"$1"]
        }
      ]

    :mnesia.dirty_select(run_opts[:instance], match_spec)
  end

  defp do_search({op, %AttributePath{attribute: attribute} = attr_path, value},
                 run_opts)
  when is_integer(value) do
    match_spec =
      [
        {
          {
            run_opts[:instance],
            :"$1",
            attribute,
            attribute_path_to_match_spec_field(attr_path)
          },
          [{:is_integer, :"$2"}, {op_to_match_spec_atom_op(op), :"$2", value}],
          [:"$1"]
        }
      ]

    :mnesia.dirty_select(run_opts[:instance], match_spec)
  end

  defp do_search({op, %AttributePath{attribute: attribute} = attr_path, %DateTime{} = value},
                 run_opts) do
    match_spec =
      [
        {
          {
            run_opts[:instance],
            :"$1",
            attribute,
            attribute_path_to_match_spec_field_datetime(attr_path)
          },
          [{op_to_match_spec_atom_op(op), :"$2", DateTime.to_unix(value)}],
          [:"$1"]
        }
      ]

    :mnesia.dirty_select(run_opts[:instance], match_spec)
  end

  defp do_search({:pr, %AttributeRepository.Search.AttributePath{
    attribute: attribute,
    sub_attribute: nil
  }}, run_opts) do
    :mnesia.dirty_match_object({run_opts[:instance], :"_", attribute, :"_"})
    |> Enum.filter(
      fn
        {_table, _resource_id, _attribute, nil} -> false
        {_table, _resource_id, _attribute, _} -> true
      end
    )
    |> Enum.map(
      fn
        {_table, resource_id, _attribute, _value} -> resource_id
      end
    )
  end

  defp do_search({:pr, %AttributePath{
    attribute: attribute,
    sub_attribute: sub_attribute
  }}, run_opts) do
    :mnesia.dirty_match_object({run_opts[:instance], :"_", attribute, %{sub_attribute => :"_"}})
    |> Enum.filter(
      fn
        {_table, _resource_id, _attribute, %{^sub_attribute => nil}} -> false
        {_table, _resource_id, _attribute, _} -> true
      end
    )
    |> Enum.map(
      fn
        {_table, resource_id, _attribute, _value} -> resource_id
      end
    )
  end

  defp do_search({:valuePath, %AttributePath{attribute: attribute}, val_filter}, run_opts) do
    match_spec =
      Enum.map(
        build_value_path(val_filter, 2),
        fn {match_map, guard} ->
          {
            {
              run_opts[:instance],
              :"$1",
              attribute,
              match_map
            },
            [guard],
            [:"$1"]
          }
        end
      )

    :mnesia.dirty_select(run_opts[:instance], match_spec)
  end

  # A few examples of the call of the build_value_path function
  #
  # iex(56)> AttributeRepositoryMnesia.build_value_path(:filter.parse(:filter_lexer.string('map[a eq 1]') |> elem(1)) |> elem(1) |> elem(2), 2)
  # [{%{"a" => :"$2"}, {:==, :"$2", 1}}]
  # iex(57)> AttributeRepositoryMnesia.build_value_path(:filter.parse(:filter_lexer.string('map[a eq 1 and b eq 2]') |> elem(1)) |> elem(1) |> elem(2), 2)
  # [{%{"a" => :"$2", "b" => :"$3"}, {:andalso, {:==, :"$2", 1}, {:==, :"$3", 2}}}]
  # iex(58)> AttributeRepositoryMnesia.build_value_path(:filter.parse(:filter_lexer.string('map[a eq 1 or b eq 2]') |> elem(1)) |> elem(1) |> elem(2), 2)
  # [{%{"a" => :"$2"}, {:==, :"$2", 1}}, {%{"b" => :"$3"}, {:==, :"$3", 2}}]
  # iex(59)> AttributeRepositoryMnesia.build_value_path(:filter.parse(:filter_lexer.string('map[a eq 1 and b eq 2 and c eq 3]') |> elem(1)) |> elem(1) |> elem(2), 2)
  # [
  #   {%{"a" => :"$2", "b" => :"$3", "c" => :"$4"},
  #    {:andalso, {:==, :"$2", 1}, {:andalso, {:==, :"$3", 2}, {:==, :"$4", 3}}}}
  # ]
  # iex(60)> AttributeRepositoryMnesia.build_value_path(:filter.parse(:filter_lexer.string('map[a eq 1 and b eq 2 or c eq 3]') |> elem(1)) |> elem(1) |> elem(2), 2)
  # [
  #   {%{"a" => :"$2", "b" => :"$3"}, {:andalso, {:==, :"$2", 1}, {:==, :"$3", 2}}},
  #   {%{"a" => :"$2", "c" => :"$4"}, {:andalso, {:==, :"$2", 1}, {:==, :"$4", 3}}}
  # ]


  defp build_value_path({:and, lhs, rhs}, match_seq_n) do
    for {lhs_match_map, lhs_guard} <- build_value_path(lhs, match_seq_n),
        {rhs_match_map, rhs_guard} <- build_value_path(rhs, match_seq_n + 1) do
      {
        Map.merge(lhs_match_map, rhs_match_map),
        {:andalso, lhs_guard, rhs_guard}
      }
    end
  end

  defp build_value_path({:or, lhs, rhs}, match_seq_n) do
    # we don't use the :orelse guard here, because it forces us to have a match map
    # that contain all the keys. When one key doesn't exist, there is no match even
    # if one side of the orelse evaluates to true
    build_value_path(lhs, match_seq_n) ++ build_value_path(rhs, match_seq_n + 1)
  end

  defp build_value_path({op, attr_path, value}, match_seq_n) when is_binary(value) do
    match_var = :"$#{match_seq_n}"

    [{
      %{attr_path.attribute => match_var},
      {:andalso, {:is_binary, match_var}, {op_to_match_spec_atom_op(op), match_var, value}}
    }]
  end

  defp build_value_path({op, attr_path, value}, match_seq_n)
  when is_boolean(value) and op in [:eq, :ne] do
    match_var = :"$#{match_seq_n}"

    [{
      %{attr_path.attribute => match_var},
      {:andalso, {:is_atom, match_var}, {op_to_match_spec_atom_op(op), match_var, value}}
    }]
  end

  defp build_value_path({_op, _attr_path, value}, _match_seq_n) when is_boolean(value) do
    raise "Invalid operation for boolean"
  end

  defp build_value_path({op, attr_path, value}, match_seq_n) when is_float(value) do
    match_var = :"$#{match_seq_n}"

    [{
      %{attr_path.attribute => match_var},
      {:andalso, {:is_float, match_var}, {op_to_match_spec_atom_op(op), match_var, value}}
    }]
  end

  defp build_value_path({op, attr_path, value}, match_seq_n) when is_integer(value) do
    match_var = :"$#{match_seq_n}"

    [{
      %{attr_path.attribute => match_var},
      {:andalso, {:is_integer, match_var}, {op_to_match_spec_atom_op(op), match_var, value}}
    }]
  end

  defp build_value_path({op, attr_path, %DateTime{} = value}, match_seq_n) do
    match_var = :"$#{match_seq_n}"

    [{
      %{attr_path.attribute => {:datetime, match_var}},
      {op_to_match_spec_atom_op(op), match_var, DateTime.to_unix(value)}
    }]
  end

  defp build_value_path({:pr, attr_path}, match_seq_n) do
    match_var = :"$#{match_seq_n}"

    [{
      %{attr_path.attribute => match_var},
      {:"/=", match_var, :nil}
    }]
  end

  defp attribute_path_to_match_spec_field(%AttributePath{
    sub_attribute: nil
  }) do
    :"$2"
  end

  defp attribute_path_to_match_spec_field(%AttributePath{
    sub_attribute: sub_attribute
  }) do
    %{sub_attribute => :"$2"}
  end

  defp attribute_path_to_match_spec_field_datetime(%AttributePath{
    sub_attribute: nil
  }) do
    {:datetime, :"$2"}
  end

  defp attribute_path_to_match_spec_field_datetime(%AttributePath{
    sub_attribute: sub_attribute
  }) do
    %{sub_attribute => {:datetime, :"$2"}}
  end

  defp op_to_match_spec_atom_op(:eq), do: :"=="
  defp op_to_match_spec_atom_op(:ne), do: :"/="
  defp op_to_match_spec_atom_op(:gt), do: :">"
  defp op_to_match_spec_atom_op(:ge), do: :">="
  defp op_to_match_spec_atom_op(:lt), do: :"<"
  defp op_to_match_spec_atom_op(:le), do: :"=<"

  defp negate_expression({:attrExp, attr_exp}) do
    {:attrExp, negate_expression(attr_exp)}
  end

  defp negate_expression({:not, exp}) do
    negate_expression(exp)
  end

  defp negate_expression({:pr, _attr_path}) do
    raise "Unsupported"
  end

  defp negate_expression({:eq, attr_path, comp_value}) do
    {:ne, attr_path, comp_value}
  end

  defp negate_expression({:ne, attr_path, comp_value}) do
    {:eq, attr_path, comp_value}
  end

  defp negate_expression({:gt, attr_path, comp_value}) do
    {:le, attr_path, comp_value}
  end

  defp negate_expression({:ge, attr_path, comp_value}) do
    {:lt, attr_path, comp_value}
  end

  defp negate_expression({:lt, attr_path, comp_value}) do
    {:ge, attr_path, comp_value}
  end

  defp negate_expression({:le, attr_path, comp_value}) do
    {:gt, attr_path, comp_value}
  end

  defp negate_expression({:and, filter_l, filter_r}) do
    {:or, filter_l, filter_r}
  end

  defp negate_expression({:or, filter_l, filter_r}) do
    {:and, filter_l, filter_r}
  end

  defp negate_expression({:valuePath, attr_path, val_filter}) do
    {:valuePath, attr_path, negate_expression(val_filter)}
  end
end
