defmodule AttributeRepositoryMnesia do
  @moduledoc """
  Documentation for AttributeRepositoryMnesia.
  """

  require Logger

  @behaviour AttributeRepository.Install
  @behaviour AttributeRepository.Write

  @impl AttributeRepository.Install

  def install(run_opts, init_opts) do
    :mnesia.stop()

    :mnesia.create_schema([node()])

    :mnesia.start()

    case :mnesia.create_table(run_opts[:instance], [
      attributes: [:key,
                   :attrs,
                   :created,
                   :last_modified,
                   :history
      ]
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

  @impl AttributeRepository.Write

  def put(resource_id, resource, run_opts) do
    case :mnesia.transaction(fn ->
      :mnesia.write({
        run_opts[:instance],
        resource_id,
        resource,
        now(),
        now(),
        nil
      })
    end) do
      {:atomic, :ok} ->
        Logger.debug("#{__MODULE__}: written #{inspect resource} " <>
          "for instance #{run_opts[:instance]}")

        {:ok, resource}

      {:aborted, reason} ->
        Logger.error("#{__MODULE__}: failed to write #{inspect resource} for instance " <>
          "#{run_opts[:instance]} (reason: #{inspect reason})")

        {:error, AttributeRepository.WriteError.exception([])}
    end
  end

  @impl AttributeRepository.Write

  def delete(resource_id, run_opts) do
    case :mnesia.transaction(fn ->
      :mnesia.delete({run_opts[:instance], resource_id})
    end) do
      {:atomic, :ok} ->
        Logger.debug("#{__MODULE__}: deleted #{inspect resource_id} " <>
          "for instance #{run_opts[:instance]}")

        :ok

      {:aborted, reason} ->
        Logger.error("#{__MODULE__}: failed to delete #{inspect resource_id} for instance " <>
          "#{run_opts[:instance]} (reason: #{inspect reason})")

        {:error, AttributeRepository.WriteError.exception([])}
    end
  end

  defp now(), do: System.system_time(:second)
end
