defmodule AttributeRepositoryMnesiaTest.Read do
  use ExUnit.Case

  @init_opts [instance: :test]
  @run_opts [instance: :test]

  setup_all do
    AttributeRepositoryMnesia.install(@run_opts, @init_opts)

    AttributeRepositoryMnesia.start(@init_opts)

    for i <- 1..9 do
      AttributeRepositoryMnesia.put(
        i,
        %{
          "str" => ?a..?z |> Enum.chunk_every(3) |> Enum.at(i - 1) |> List.to_string(),
          "bool" => rem(i, 3) == 0,
          "float" => i * 1.0,
          "int" => i,
          "datetime" => DateTime.from_iso8601("#{2000 + i}-01-23T23:50:07Z") |> elem(1),
          "complex" => %{
            "str" => ?a..?z |> Enum.chunk_every(3) |> Enum.at(i - 1) |> List.to_string(),
            "bool" => rem(i, 3) == 0,
            "float" => i * 1.0,
            "int" => i,
            "datetime" => DateTime.from_iso8601("#{2000 + i}-01-23T23:50:07Z") |> elem(1),
          }
        },
        @run_opts)
    end

    AttributeRepositoryMnesia.modify(3,
                                     [
                                       {:add, "multval", "Hi"},
                                       {:add, "multval", "Salut"},
                                       {:add, "multval", "Привет"}
                                     ], @run_opts)

    AttributeRepositoryMnesia.modify(4, [{:add, "multval", "Hi"},
                                         {:add, "multval", "Salut"}], @run_opts)

    AttributeRepositoryMnesia.modify(1, [{:add, "nil_attr", nil},
                                         {:add, "nil_attr_complex", %{"nil_attr" => nil, "not_nil_attr" => "a value"}}],
                                         @run_opts)
  end

  test "Basic get" do
    {:ok, res} = AttributeRepositoryMnesia.get(1, :all, @run_opts)

    assert res["str"] == "abc"
    assert res["bool"] == false
    assert res["float"] === 1.0
    assert res["int"] === 1
    assert DateTime.compare(res["datetime"],
                            elem(DateTime.from_iso8601("2001-01-23T23:50:07Z"), 1))
    assert res["complex"]["str"] == "abc"
    assert res["complex"]["bool"] == false
    assert res["complex"]["float"] === 1.0
    assert res["complex"]["int"] === 1
    assert DateTime.compare(res["complex"]["datetime"],
                            elem(DateTime.from_iso8601("2001-01-23T23:50:07Z"), 1))
  end

  test "Get with no attribute returned" do
    attrs = ["nonexisting_attr_1", "nonexisting_attr_2"]

    assert {:ok, _} = AttributeRepositoryMnesia.get(1, attrs, @run_opts)
  end

  test "Get a multi-valued attribute with one value returns a list" do
    AttributeRepositoryMnesia.put(10, %{"multivalued_1" => ["single_value"]}, @run_opts)

    {:ok, res} = AttributeRepositoryMnesia.get(10, ["multivalued_1"], @run_opts)

    assert res["multivalued_1"] == ["single_value"]
  end

  test "Get a multi-valued attribute with no value returns an empty list" do
    AttributeRepositoryMnesia.put(11, %{"multivalued_2" => []}, @run_opts)

    {:ok, res} = AttributeRepositoryMnesia.get(11, ["multivalued_2"], @run_opts)

    assert res["multivalued_2"] == []
  end

  test "Multival get" do
    {:ok, res} = AttributeRepositoryMnesia.get(3, :all, @run_opts)

    assert res["multval"] in [
      ["Hi", "Salut", "Привет"],
      ["Hi", "Привет", "Salut"],
      ["Salut", "Привет", "Hi"],
      ["Привет", "Salut", "Hi"],
      ["Привет", "Hi", "Salut"],
      ["Salut", "Hi", "Привет"]
    ]
  end

end
