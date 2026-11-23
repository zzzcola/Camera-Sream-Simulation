CXX      = g++
CXXFLAGS = -std=c++11 -pthread -I./libtime

camera_simulator: base_camera.cpp libtime/libtime.cpp
	$(CXX) $(CXXFLAGS) base_camera.cpp libtime/libtime.cpp -o camera_simulator
clean:
	rm -f camera_simulator