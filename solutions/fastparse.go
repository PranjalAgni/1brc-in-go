package solutions

import (
	"bufio"
	"fmt"
	"math"
	"os"
	"sort"
	"strings"
)

func FastTemperatureParse(filePath string) error {

	filename := filePath

	// Open the file
	file, err := os.Open(filename)
	if err != nil {
		fmt.Println("Error opening file:", err)
		return err
	}

	defer file.Close()

	// finalResult map
	finalResult := make(map[string]*ResultMap)

	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		line := scanner.Text()
		station, tempStr, hasSemi := strings.Cut(line, ";")
		if !hasSemi {
			continue
		}

		f := parseTemperature(tempStr)

		existingValue, ok := finalResult[station]

		if ok {
			existingValue.Min = math.Min(existingValue.Min, f)
			existingValue.Max = math.Max(existingValue.Max, f)
			existingValue.Count += 1
			existingValue.Sum += f
		} else {
			finalResult[station] = &ResultMap{
				Min:   f,
				Max:   f,
				Count: 1,
				Sum:   f,
			}
		}
	}

	stations := make([]string, 0, len(finalResult))
	for station := range finalResult {
		stations = append(stations, station)
	}

	sort.Strings(stations)

	fmt.Print("{")
	for i, station := range stations {
		if i > 0 {
			fmt.Print(", ")
		}
		result := finalResult[station]
		fmt.Printf("%s=", station)
		fmt.Printf("%.1f/", result.Min)
		fmt.Printf("%.1f/", result.Sum/float64(result.Count))
		fmt.Printf("%.1f", result.Max)
	}

	fmt.Print("}\n")
	return nil
}

func parseTemperature(tempStr string) float64 {
	negative := false
	index := 0

	if tempStr[index] == '-' {
		index += 1
		negative = true
	}

	temp := float64(tempStr[index] - '0')
	index += 1
	if tempStr[index] != '.' {
		temp = temp*10 + float64(tempStr[index]-'0') // parse optional second digit
		index += 1
	}

	index += 1 // skip .

	temp += float64(tempStr[index]-'0') / 10
	if negative {
		temp = -temp
	}

	return temp
}
