defmodule AttributeRepositoryMnesia do
  @moduledoc """
  Documentation for AttributeRepositoryMnesia.
  """

  require Logger

  use AttributeRepository.Read
  use AttributeRepository.Write

  alias AttributeRepository.Search.AttributePath

  @behaviour AttributeRepository.Install
  @behaviour AttributeRepository.Read
  @behaviour AttributeRepository.Write
  @behaviour AttributeRepository.Search

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
        Logger.debug("#{__MODULE__}: created table for instance #{run_opts[:instance]}")

        :ok

      {:aborted, reason} ->
        Logger.error("#{__MODULE__}: failed to create table for instance " <>
          "#{run_opts[:instance]} (reason: #{inspect reason})")

        {:error, reason}
    end

  end

  @impl AttributeRepository.Read

  def get(resource_id, :all, run_opts) do
    case :mnesia.transaction(fn -> :mnesia.read(run_opts[:instance], resource_id) end) do
      {:atomic, [_element | _] = record_list} ->
        {:ok,
          Enum.reduce(
            record_list,
            %{},
            fn
              {_table, _resource_id, attribute, {:datetime, dt_value}}, res ->
                Map.put(res, attribute, elem(DateTime.from_unix(dt_value), 1))

              {_table, _resource_id, attribute, value}, res ->
                Map.put(res, attribute, value)
            end
          )
        }

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
      {:atomic, [_element | _] = object_list} ->
        {:ok,
          Enum.reduce(
            object_list,
            %{},
            fn
              {_table, _resource_id, attribute, {:datetime, dt_value}}, res ->
                Map.put(res, attribute, elem(DateTime.from_unix(dt_value), 1))

              {_table, _resource_id, attribute, value}, res ->
                Map.put(res, attribute, value)
            end
          )
        }

      {:atomic, []} ->
        {:error, AttributeRepository.Read.NotFoundError.exception([])}

      _ ->
        {:error, AttributeRepository.ReadError.exception([])}
    end
  end
  @impl AttributeRepository.Write

  def put(resource_id, %{} = resource, run_opts) do
    case :mnesia.transaction(fn ->
      for {attr, val} <- resource do
        :mnesia.write({
          run_opts[:instance],
          resource_id,
          attr,
          case val do
            %DateTime{} ->
              {:datetime, DateTime.to_unix(val)}

            _ ->
              val
          end
        })
      end
    end) do
      {:atomic, _} ->
        Logger.debug("#{__MODULE__}: written `#{inspect resource}` " <>
          "for resource_id `#{inspect resource_id}` " <>
          "for instance #{run_opts[:instance]}")

        {:ok, resource}

      {:aborted, reason} ->
        Logger.error("#{__MODULE__}: failed to write #{inspect resource} " <>
          "for resource_id `#{inspect(resource_id)}` " <>
          "for instance #{run_opts[:instance]} (reason: #{inspect reason})")

        {:error, AttributeRepository.WriteError.exception([])}
    end
  end

  @impl AttributeRepository.Write

  def delete(resource_id, run_opts) do
    case :mnesia.transaction(fn ->
      :mnesia.delete({run_opts[:instance], resource_id})
    end) do
      {:atomic, _} ->
        Logger.debug("#{__MODULE__}: deleted resource_id `#{inspect resource_id}` " <>
          "for instance #{run_opts[:instance]}")

        :ok

      {:aborted, reason} ->
        Logger.error("#{__MODULE__}: failed to delete resource_id `#{inspect resource_id}` " <>
          "for instance #{run_opts[:instance]} (reason: #{inspect reason})")

        {:error, AttributeRepository.WriteError.exception([])}
    end
  end

  @impl AttributeRepository.Search

  def search(filter, _attributes, run_opts) do
    do_search(filter, run_opts)
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

  defp do_search({:eq, %AttributePath{attribute: attribute} = attr_path, value},
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
          [{:"==", :'$2', value}],
          [:"$1"]
        }
      ]

    :mnesia.dirty_select(run_opts[:instance], match_spec)
  end

  defp do_search({:eq, %AttributePath{attribute: attribute} = attr_path, value},
                 run_opts)
  when value in [true, false] do
    match_spec =
      [
        {
          {
            run_opts[:instance],
            :"$1",
            attribute,
            attribute_path_to_match_spec_field(attr_path)
          },
          [{:"==", :'$2', value}],
          [:"$1"]
        }
      ]

    :mnesia.dirty_select(run_opts[:instance], match_spec)
  end

  defp do_search({:eq, %AttributePath{attribute: attribute} = attr_path, value},
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
          [{:is_float, :"$2"}, {:"==", :'$2', value}],
          [:"$1"]
        }
      ]

    :mnesia.dirty_select(run_opts[:instance], match_spec)
  end

  defp do_search({:eq, %AttributePath{attribute: attribute} = attr_path, value},
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
          [{:is_integer, :"$2"}, {:"==", :"$2", value}],
          [:"$1"]
        }
      ]

    :mnesia.dirty_select(run_opts[:instance], match_spec)
  end

  defp do_search({:eq, %AttributePath{attribute: attribute} = attr_path, %DateTime{} = value},
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
          [{:"==", :"$2", value}],
          [:"$1"]
        }
      ]

    :mnesia.dirty_select(run_opts[:instance], match_spec)
  end

  defp do_search({:ne, %AttributePath{attribute: attribute} = attr_path, value},
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
          [{:"=/=", :'$2', value}],
          [:"$1"]
        }
      ]

    :mnesia.dirty_select(run_opts[:instance], match_spec)
  end

  defp do_search({:ne, %AttributePath{attribute: attribute} = attr_path, value},
                 run_opts)
  when value in [true, false] do
    match_spec =
      [
        {
          {
            run_opts[:instance],
            :"$1",
            attribute,
            attribute_path_to_match_spec_field(attr_path)
          },
          [{:"==", :"$2", !value}],
          [:"$1"]
        }
      ]

    :mnesia.dirty_select(run_opts[:instance], match_spec)
  end

  defp do_search({:ne, %AttributePath{attribute: attribute} = attr_path, value},
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
          [{:is_float, :"$2"}, {:"/=", :'$2', value}],
          [:"$1"]
        }
      ]

    :mnesia.dirty_select(run_opts[:instance], match_spec)
  end

  defp do_search({:ne, %AttributePath{attribute: attribute} = attr_path, value},
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
          [{:is_integer, :"$2"}, {:"/=", :'$2', value}],
          [:"$1"]
        }
      ]

    :mnesia.dirty_select(run_opts[:instance], match_spec)
  end

  defp do_search({:ne, %AttributePath{attribute: attribute} = attr_path, %DateTime{} = value},
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
          [{:"/=", :'$2', DateTime.to_unix(value)}],
          [:"$1"]
        }
      ]

    :mnesia.dirty_select(run_opts[:instance], match_spec)
  end

  defp do_search({:gt, %AttributePath{attribute: attribute} = attr_path, value},
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
          [{:">", :'$2', value}],
          [:"$1"]
        }
      ]

    :mnesia.dirty_select(run_opts[:instance], match_spec)
  end

  defp do_search({:gt, %AttributePath{}, value}, _)
  when value in [true, false] do
    raise "Invalid operation for boolean"
  end

  defp do_search({:gt, %AttributePath{attribute: attribute}, value} = attr_path,
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
          [{:is_float, :"$2"}, {:">", :'$2', value}],
          [:"$1"]
        }
      ]

    :mnesia.dirty_select(run_opts[:instance], match_spec)
  end

  defp do_search({:gt, %AttributePath{attribute: attribute} = attr_path, value},
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
          [{:is_integer, :"$2"}, {:">", :'$2', value}],
          [:"$1"]
        }
      ]

    :mnesia.dirty_select(run_opts[:instance], match_spec)
  end

  defp do_search({:gt, %AttributePath{attribute: attribute} = attr_path, %DateTime{} = value},
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
          [{:">", :'$2', DateTime.to_unix(value)}],
          [:"$1"]
        }
      ]

    :mnesia.dirty_select(run_opts[:instance], match_spec)
  end

  defp do_search({:ge, %AttributePath{attribute: attribute} = attr_path, value},
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
          [{:">=", :'$2', value}],
          [:"$1"]
        }
      ]

    :mnesia.dirty_select(run_opts[:instance], match_spec)
  end

  defp do_search({:ge, %AttributePath{}, value}, _)
  when value in [true, false] do
    raise "Invalid operation for boolean"
  end

  defp do_search({:ge, %AttributePath{attribute: attribute} = attr_path, value},
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
          [{:is_float, :"$2"}, {:">=", :'$2', value}],
          [:"$1"]
        }
      ]

    :mnesia.dirty_select(run_opts[:instance], match_spec)
  end

  defp do_search({:ge, %AttributePath{attribute: attribute} = attr_path, value},
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
          [{:is_integer, :"$2"}, {:">=", :'$2', value}],
          [:"$1"]
        }
      ]

    :mnesia.dirty_select(run_opts[:instance], match_spec)
  end

  defp do_search({:ge, %AttributePath{attribute: attribute} = attr_path, %DateTime{} = value},
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
          [{:">=", :'$2', DateTime.to_unix(value)}],
          [:"$1"]
        }
      ]

    :mnesia.dirty_select(run_opts[:instance], match_spec)
  end

  defp do_search({:lt, %AttributePath{attribute: attribute} = attr_path, value},
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
          [{:"<", :'$2', value}],
          [:"$1"]
        }
      ]

    :mnesia.dirty_select(run_opts[:instance], match_spec)
  end

  defp do_search({:lt, %AttributePath{}, value}, _)
  when value in [true, false] do
    raise "Invalid operation for boolean"
  end

  defp do_search({:lt, %AttributePath{attribute: attribute} = attr_path, value},
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
          [{:is_float, :"$2"}, {:"<", :'$2', value}],
          [:"$1"]
        }
      ]

    :mnesia.dirty_select(run_opts[:instance], match_spec)
  end

  defp do_search({:lt, %AttributePath{attribute: attribute} = attr_path, value},
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
          [{:is_integer, :"$2"}, {:"<", :'$2', value}],
          [:"$1"]
        }
      ]

    :mnesia.dirty_select(run_opts[:instance], match_spec)
  end

  defp do_search({:lt, %AttributePath{attribute: attribute} = attr_path, %DateTime{} = value},
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
          [{:"<", :'$2', DateTime.to_unix(value)}],
          [:"$1"]
        }
      ]

    :mnesia.dirty_select(run_opts[:instance], match_spec)
  end

  defp do_search({:le, %AttributePath{attribute: attribute} = attr_path, value},
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
          [{:"=<", :'$2', value}],
          [:"$1"]
        }
      ]

    :mnesia.dirty_select(run_opts[:instance], match_spec)
  end

  defp do_search({:le, %AttributePath{}, value}, _)
  when value in [true, false] do
    raise "Invalid operation for boolean"
  end

  defp do_search({:le, %AttributePath{attribute: attribute} = attr_path, value},
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
          [{:is_float, :"$2"}, {:"=<", :'$2', value}],
          [:"$1"]
        }
      ]

    :mnesia.dirty_select(run_opts[:instance], match_spec)
  end

  defp do_search({:le, %AttributePath{attribute: attribute} = attr_path, value},
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
          [{:is_integer, :"$2"}, {:"=<", :'$2', value}],
          [:"$1"]
        }
      ]

    :mnesia.dirty_select(run_opts[:instance], match_spec)
  end

  defp do_search({:le, %AttributePath{attribute: attribute} = attr_path, %DateTime{} = value},
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
          [{:"=<", :'$2', DateTime.to_unix(value)}],
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

  defp do_search({:pr, %AttributeRepository.Search.AttributePath{
    attribute: attribute,
    sub_attribute: sub_attribute
  }}, run_opts) do
    :mnesia.dirty_match_object({run_opts[:instance], :"_", attribute, %{sub_attribute => :"_"}})
    |> Enum.filter(
      fn
        {_table, _resource_id, _attribute, %{sub_attribute: nil}} -> false
        {_table, _resource_id, _attribute, _} -> true
      end
    )
    |> Enum.map(
      fn
        {_table, resource_id, _attribute, _value} -> resource_id
      end
    )
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
end
