import envoy
import gleam/option.{type Option, None, Some}
import gleam/result
import gleam/string
import simplifile

/// Environment and mounted secret files are mutually exclusive. Error text
/// names the setting only, never its value or filesystem contents.
pub fn optional(name: String) -> Result(Option(String), String) {
  case envoy.get(name), envoy.get(name <> "_FILE") {
    Ok(_), Ok(_) -> Error("set only " <> name <> " or its _FILE setting")
    Ok(value), Error(_) -> valid(name, value) |> result.map(Some)
    Error(_), Ok(path) -> {
      use info <- result.try(
        simplifile.file_info(path)
        |> result.replace_error("cannot read " <> name <> "_FILE"),
      )
      use _ <- result.try(case info.size <= 8192 {
        True -> Ok(Nil)
        False -> Error("oversized " <> name <> "_FILE")
      })
      use value <- result.try(
        simplifile.read(path)
        |> result.replace_error("cannot read " <> name <> "_FILE"),
      )
      valid(name, string.trim(value)) |> result.map(Some)
    }
    Error(_), Error(_) -> Ok(None)
  }
}

fn valid(name: String, value: String) -> Result(String, String) {
  case value != "" && string.byte_size(value) <= 8192 {
    True -> Ok(value)
    False -> Error("invalid empty or oversized " <> name)
  }
}

pub fn required(name: String) -> Result(String, String) {
  use value <- result.try(optional(name))
  case value {
    Some(value) -> Ok(value)
    None -> Error(name <> " is required")
  }
}
