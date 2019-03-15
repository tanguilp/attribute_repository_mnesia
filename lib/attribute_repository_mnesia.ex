defmodule AttributeRepositoryMnesia do
  @moduledoc """
  Documentation for AttributeRepositoryMnesia.
  """

  require Logger

  use AttributeRepository.Read
  use AttributeRepository.Write

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
              {_table, _resource_id, attribute, value}, res -> Map.put(res, attribute, value)
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
  # it now returns NotFoundError exception but should not

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
        {:ok, Enum.into(object_list, %{})}

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
          val
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
  end
end
