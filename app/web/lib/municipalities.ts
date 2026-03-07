const SUPPORTED_MUNICIPALITIES = new Set(["MBJB", "MBPJ"]);

export function parseMunicipalities(searchParams: URLSearchParams) {
  const values = searchParams
    .getAll("municipality")
    .flatMap((item) => item.split(","))
    .map((item) => item.trim().toUpperCase())
    .filter((item) => SUPPORTED_MUNICIPALITIES.has(item));

  return values.length ? [...new Set(values)] : ["MBJB"];
}
