defmodule AttributeRepositoryMnesiaTest.Write do
  use ExUnit.Case

  @init_opts [instance: :test]
  @run_opts [instance: :test]

  setup_all do
    AttributeRepositoryMnesia.install(@run_opts, @init_opts)

    AttributeRepositoryMnesia.start(@init_opts)
  end

  setup do
    AttributeRepositoryMnesia.delete("test_write", @run_opts)

    AttributeRepositoryMnesia.put("test_write", %{
      "key1" => "value 1",
      "key2" => 2,
      "key3" => true,
      "key4" => ["val1", "val2", "val3", "val4"]
    }, @run_opts)
  end

  # put

  test "put 1" do
    obj = AttributeRepositoryMnesia.get!("test_write", :all, @run_opts)

    assert obj["key1"] == "value 1"
    assert obj["key2"] == 2
    assert obj["key3"] == true
    assert Enum.sort(obj["key4"]) == ["val1", "val2", "val3", "val4"]

    AttributeRepositoryMnesia.put("test_write",
                                  %{"key1" => "another value", "key4" => "value"},
                                  @run_opts)

    obj = AttributeRepositoryMnesia.get!("test_write", :all, @run_opts)

    assert obj["key1"] == "another value"
    assert obj["key2"] == nil
    assert obj["key3"] == nil
    assert obj["key4"] == "value"
  end

  test "modify add nonexistant value" do
    AttributeRepositoryMnesia.modify(
      "test_write",
      [
        {:add, "key5", "value 5"}
      ],
      @run_opts
    )

    obj = AttributeRepositoryMnesia.get!("test_write", :all, @run_opts)

    assert obj["key5"] == "value 5"
  end

  test "modify add value to existant single attribute" do
    AttributeRepositoryMnesia.modify(
      "test_write",
      [
        {:add, "key1", "value 2"}
      ],
      @run_opts
    )

    obj = AttributeRepositoryMnesia.get!("test_write", :all, @run_opts)

    assert Enum.sort(obj["key1"]) == ["value 1", "value 2"]
  end

  test "modify add value to existant multivalued attribute" do
    AttributeRepositoryMnesia.modify(
      "test_write",
      [
        {:add, "key4", "val5"}
      ],
      @run_opts
    )

    obj = AttributeRepositoryMnesia.get!("test_write", :all, @run_opts)

    assert Enum.sort(obj["key4"]) == ["val1", "val2", "val3", "val4", "val5"]
  end

  test "modify replace value to nonexisting simple attribute" do
    AttributeRepositoryMnesia.modify(
      "test_write",
      [
        {:replace, "key5", "some value"}
      ],
      @run_opts
    )

    obj = AttributeRepositoryMnesia.get!("test_write", :all, @run_opts)

    assert obj["key5"] == "some value"
  end

  test "modify replace value to existing simple attribute" do
    AttributeRepositoryMnesia.modify(
      "test_write",
      [
        {:replace, "key2", "some value"}
      ],
      @run_opts
    )

    obj = AttributeRepositoryMnesia.get!("test_write", :all, @run_opts)

    assert obj["key2"] == "some value"
  end

  test "modify replace value to existing mutlivalued attribute" do
    AttributeRepositoryMnesia.modify(
      "test_write",
      [
        {:replace, "key4", "some value"}
      ],
      @run_opts
    )

    obj = AttributeRepositoryMnesia.get!("test_write", :all, @run_opts)

    assert obj["key4"] == "some value"
  end

  test "modify replace one specific value to existing mutlivalued attribute" do
    AttributeRepositoryMnesia.modify(
      "test_write",
      [
        {:replace, "key4", "val2", "val5"}
      ],
      @run_opts
    )

    obj = AttributeRepositoryMnesia.get!("test_write", :all, @run_opts)

    assert Enum.sort(obj["key4"]) == ["val1", "val3", "val4", "val5"]
  end

  test "modify delete value to existant simple attribute" do
    AttributeRepositoryMnesia.modify(
      "test_write",
      [
        {:delete, "key1"}
      ],
      @run_opts
    )

    obj = AttributeRepositoryMnesia.get!("test_write", :all, @run_opts)

    assert obj["key1"] == nil
  end

  test "modify delete all values to existant mutlivalued attribute" do
    AttributeRepositoryMnesia.modify(
      "test_write",
      [
        {:delete, "key4"}
      ],
      @run_opts
    )

    obj = AttributeRepositoryMnesia.get!("test_write", :all, @run_opts)

    assert obj["key4"] == nil
  end

  test "modify delete one value to existant mutlivalued attribute" do
    AttributeRepositoryMnesia.modify(
      "test_write",
      [
        {:delete, "key4", "val3"}
      ],
      @run_opts
    )

    obj = AttributeRepositoryMnesia.get!("test_write", :all, @run_opts)

    assert Enum.sort(obj["key4"]) == ["val1", "val2", "val4"]
  end

  test "delete" do
    AttributeRepositoryMnesia.put("test_delete",
                                  %{"key1" => "value", "key2" => "value"},
                                  @run_opts)

    assert {:ok, _} = AttributeRepositoryMnesia.get("test_delete", :all, @run_opts)

    assert :ok == AttributeRepositoryMnesia.delete("test_delete", @run_opts)

    assert {:error, %AttributeRepository.Read.NotFoundError{}} =
      AttributeRepositoryMnesia.get("test_delete", :all, @run_opts)
  end
end
